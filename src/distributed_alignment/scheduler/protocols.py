"""WorkStack protocol — the interface for work package distribution.

Any backend (filesystem, S3, Redis, SQS) implements this protocol.
Application code only depends on the protocol, not any specific backend.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from distributed_alignment.models import ChunkManifest, WorkPackage


class WorkStack(Protocol):
    """Protocol for distributing work packages to workers.

    Implementations must provide atomic claim semantics — no two
    workers can successfully claim the same package.
    """

    def generate_work_packages(
        self,
        query_manifest: ChunkManifest,
        ref_manifest: ChunkManifest,
        *,
        max_attempts: int = 3,
    ) -> list[WorkPackage]:
        """Create work packages from the Cartesian product of query × ref chunks.

        Args:
            query_manifest: Manifest of query chunks.
            ref_manifest: Manifest of reference chunks.
            max_attempts: Maximum retry attempts per package.

        Returns:
            List of created WorkPackage objects, all in PENDING state.
        """
        ...

    def claim(self, worker_id: str) -> WorkPackage | None:
        """Atomically claim the next available work package.

        Args:
            worker_id: Identifier of the claiming worker.

        Returns:
            The claimed WorkPackage with state RUNNING, or None
            if no packages are available.
        """
        ...

    def complete(self, package_id: str, result_path: str) -> None:
        """Mark a work package as successfully completed.

        Args:
            package_id: ID of the package to complete.
            result_path: Path to the result file.
        """
        ...

    def fail(self, package_id: str, error: str) -> None:
        """Mark a work package as failed.

        If retries remain, the package returns to PENDING.
        If max attempts are exhausted, the package moves to POISONED.

        Args:
            package_id: ID of the failed package.
            error: Description of the failure.
        """
        ...

    def heartbeat(self, package_id: str) -> None:
        """Update the heartbeat timestamp for a running package.

        Args:
            package_id: ID of the package to heartbeat.
        """
        ...

    def reap_stale(self, timeout_seconds: int) -> list[str]:
        """Reclaim packages with stale heartbeats.

        Packages in RUNNING state whose heartbeat is older than
        ``timeout_seconds`` are moved back to PENDING (or POISONED
        if max attempts are exhausted).

        Args:
            timeout_seconds: Heartbeat staleness threshold.

        Returns:
            List of package IDs that were reaped.
        """
        ...

    def pending_count(self) -> int:
        """Return the number of packages in PENDING state."""
        ...

    def status(self) -> dict[str, int]:
        """Return counts of packages in each state.

        Returns:
            Dict mapping state name to count,
            e.g. ``{"PENDING": 5, "RUNNING": 2, "COMPLETED": 10, ...}``.
        """
        ...
