"""Filesystem-backed work stack using atomic rename for claim semantics.

Uses ``os.rename()`` from ``pending/`` to ``running/`` — atomic on POSIX.
Suitable for local development and shared-filesystem (HPC) environments.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog

from distributed_alignment.models import WorkPackage, WorkPackageState

if TYPE_CHECKING:
    from pathlib import Path

    from distributed_alignment.models import ChunkManifest

logger = structlog.get_logger()

# Directories for each work package state
STATE_DIRS: dict[WorkPackageState, str] = {
    WorkPackageState.PENDING: "pending",
    WorkPackageState.RUNNING: "running",
    WorkPackageState.COMPLETED: "completed",
    WorkPackageState.POISONED: "poisoned",
}


class FileSystemWorkStack:
    """Work stack backed by filesystem directories with atomic rename.

    Directory layout::

        base_dir/
        ├── pending/       # Packages waiting to be claimed
        ├── running/       # Packages currently being processed
        ├── completed/     # Successfully finished packages
        └── poisoned/      # Packages that exhausted all retries

    Claim atomicity relies on POSIX ``os.rename()`` — if two workers
    race to claim the same file, exactly one rename succeeds and the
    other gets ``FileNotFoundError``.
    """

    def __init__(self, base_dir: Path) -> None:
        self._base_dir = base_dir
        for dirname in STATE_DIRS.values():
            (base_dir / dirname).mkdir(parents=True, exist_ok=True)

    def _dir_for(self, state: WorkPackageState) -> Path:
        """Return the directory path for a given state."""
        return self._base_dir / STATE_DIRS[state]

    def _package_path(
        self, package_id: str, state: WorkPackageState
    ) -> Path:
        """Return the file path for a package in a given state directory."""
        return self._dir_for(state) / f"{package_id}.json"

    def _read_package(
        self, package_id: str, state: WorkPackageState
    ) -> WorkPackage:
        """Read and parse a work package JSON file."""
        path = self._package_path(package_id, state)
        data = json.loads(path.read_text())
        return WorkPackage(**data)

    def _write_package(self, package: WorkPackage) -> None:
        """Write a work package to its state directory."""
        path = self._package_path(package.package_id, package.state)
        path.write_text(
            json.dumps(package.model_dump(mode="json"), indent=2)
        )

    def _log_transition(
        self,
        package_id: str,
        from_state: WorkPackageState,
        to_state: WorkPackageState,
        *,
        worker_id: str | None = None,
        attempt: int | None = None,
        reason: str | None = None,
    ) -> None:
        """Log a state transition as a structured audit event."""
        logger.info(
            "state_transition",
            package_id=package_id,
            from_state=str(from_state),
            to_state=str(to_state),
            worker_id=worker_id,
            attempt=attempt,
            reason=reason,
            timestamp=datetime.now(tz=UTC).isoformat(),
        )

    def generate_work_packages(
        self,
        query_manifest: ChunkManifest,
        ref_manifest: ChunkManifest,
        *,
        max_attempts: int = 3,
    ) -> list[WorkPackage]:
        """Create Q×R work packages in the pending directory.

        Args:
            query_manifest: Manifest of query chunks.
            ref_manifest: Manifest of reference chunks.
            max_attempts: Maximum retry attempts per package.

        Returns:
            List of created WorkPackage objects.
        """
        packages: list[WorkPackage] = []

        for q_chunk in query_manifest.chunks:
            for r_chunk in ref_manifest.chunks:
                package_id = f"wp_{q_chunk.chunk_id}_{r_chunk.chunk_id}"
                package = WorkPackage(
                    package_id=package_id,
                    query_chunk_id=q_chunk.chunk_id,
                    ref_chunk_id=r_chunk.chunk_id,
                    state=WorkPackageState.PENDING,
                    max_attempts=max_attempts,
                )
                self._write_package(package)
                packages.append(package)

        logger.info(
            "work_packages_generated",
            count=len(packages),
            query_chunks=len(query_manifest.chunks),
            ref_chunks=len(ref_manifest.chunks),
        )

        return packages

    def claim(self, worker_id: str) -> WorkPackage | None:
        """Atomically claim the next pending work package.

        Iterates over pending package files and attempts ``os.rename()``
        to the running directory. If the rename succeeds, the package
        is claimed. If another worker got it first (``FileNotFoundError``
        or ``OSError``), tries the next file.

        Args:
            worker_id: Identifier of the claiming worker.

        Returns:
            The claimed WorkPackage or None if nothing is available.
        """
        pending_dir = self._dir_for(WorkPackageState.PENDING)
        running_dir = self._dir_for(WorkPackageState.RUNNING)

        # Sort for deterministic claim ordering
        try:
            candidates = sorted(pending_dir.iterdir())
        except FileNotFoundError:
            return None

        for src_path in candidates:
            if not src_path.name.endswith(".json"):
                continue

            dst_path = running_dir / src_path.name

            try:
                os.rename(src_path, dst_path)
            except (FileNotFoundError, OSError):
                # Another worker claimed it first — try next
                continue

            # Rename succeeded — we own this package
            package = WorkPackage(**json.loads(dst_path.read_text()))
            now = datetime.now(tz=UTC)

            self._log_transition(
                package.package_id,
                WorkPackageState.PENDING,
                WorkPackageState.RUNNING,
                worker_id=worker_id,
                attempt=package.attempt,
            )

            package.state = WorkPackageState.RUNNING
            package.claimed_by = worker_id
            package.claimed_at = now
            package.started_at = now
            package.heartbeat_at = now

            # Write updated JSON back to the running directory
            dst_path.write_text(
                json.dumps(package.model_dump(mode="json"), indent=2)
            )

            return package

        return None

    def complete(self, package_id: str, result_path: str) -> None:
        """Mark a running package as completed.

        Args:
            package_id: ID of the package to complete.
            result_path: Path to the result file produced.
        """
        src = self._package_path(package_id, WorkPackageState.RUNNING)
        dst = self._package_path(package_id, WorkPackageState.COMPLETED)

        package = WorkPackage(**json.loads(src.read_text()))

        self._log_transition(
            package_id,
            WorkPackageState.RUNNING,
            WorkPackageState.COMPLETED,
            worker_id=package.claimed_by,
            attempt=package.attempt,
            reason=f"result={result_path}",
        )

        package.state = WorkPackageState.COMPLETED
        package.completed_at = datetime.now(tz=UTC)

        dst.write_text(
            json.dumps(package.model_dump(mode="json"), indent=2)
        )
        src.unlink()

    def fail(self, package_id: str, error: str) -> None:
        """Mark a running package as failed.

        If retries remain (``attempt < max_attempts``), the package
        goes back to PENDING. Otherwise it moves to POISONED.

        Args:
            package_id: ID of the failed package.
            error: Description of the failure.
        """
        src = self._package_path(package_id, WorkPackageState.RUNNING)
        package = WorkPackage(**json.loads(src.read_text()))

        package.attempt += 1
        package.error_history.append(error)
        package.claimed_by = None
        package.claimed_at = None
        package.heartbeat_at = None
        package.started_at = None

        if package.attempt >= package.max_attempts:
            target_state = WorkPackageState.POISONED
        else:
            target_state = WorkPackageState.PENDING

        self._log_transition(
            package_id,
            WorkPackageState.RUNNING,
            target_state,
            worker_id=None,
            attempt=package.attempt,
            reason=error,
        )

        package.state = target_state
        dst = self._package_path(package_id, target_state)
        dst.write_text(
            json.dumps(package.model_dump(mode="json"), indent=2)
        )
        src.unlink()

    def heartbeat(self, package_id: str) -> None:
        """Update the heartbeat timestamp for a running package.

        Args:
            package_id: ID of the package to heartbeat.
        """
        path = self._package_path(package_id, WorkPackageState.RUNNING)
        package = WorkPackage(**json.loads(path.read_text()))
        package.heartbeat_at = datetime.now(tz=UTC)
        path.write_text(
            json.dumps(package.model_dump(mode="json"), indent=2)
        )

    def reap_stale(self, timeout_seconds: int) -> list[str]:
        """Reclaim running packages with stale heartbeats.

        Args:
            timeout_seconds: Seconds since last heartbeat before
                a package is considered stale.

        Returns:
            List of package IDs that were reaped.
        """
        running_dir = self._dir_for(WorkPackageState.RUNNING)
        now = datetime.now(tz=UTC)
        reaped: list[str] = []

        for path in sorted(running_dir.iterdir()):
            if not path.name.endswith(".json"):
                continue

            package = WorkPackage(**json.loads(path.read_text()))

            if package.heartbeat_at is None:
                continue

            age = (now - package.heartbeat_at).total_seconds()
            if age <= timeout_seconds:
                continue

            # Stale — reap it
            package.attempt += 1
            package.error_history.append(
                f"heartbeat_stale_after_{int(age)}s"
            )
            package.claimed_by = None
            package.claimed_at = None
            package.heartbeat_at = None
            package.started_at = None

            if package.attempt >= package.max_attempts:
                target_state = WorkPackageState.POISONED
            else:
                target_state = WorkPackageState.PENDING

            self._log_transition(
                package.package_id,
                WorkPackageState.RUNNING,
                target_state,
                reason=f"heartbeat_stale_after_{int(age)}s",
                attempt=package.attempt,
            )

            package.state = target_state
            dst = self._package_path(package.package_id, target_state)
            dst.write_text(
                json.dumps(package.model_dump(mode="json"), indent=2)
            )
            path.unlink()
            reaped.append(package.package_id)

        if reaped:
            logger.info(
                "packages_reaped",
                count=len(reaped),
                package_ids=reaped,
            )

        return reaped

    def pending_count(self) -> int:
        """Return the number of packages in PENDING state."""
        return self._count_in(WorkPackageState.PENDING)

    def status(self) -> dict[str, int]:
        """Return counts of packages in each state."""
        return {
            state.value: self._count_in(state) for state in STATE_DIRS
        }

    def _count_in(self, state: WorkPackageState) -> int:
        """Count JSON files in a state directory."""
        state_dir = self._dir_for(state)
        return sum(1 for f in state_dir.iterdir() if f.name.endswith(".json"))
