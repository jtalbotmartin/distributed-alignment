"""Tests for the filesystem-backed work stack."""

from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from distributed_alignment.models import (
    ChunkEntry,
    ChunkManifest,
    WorkPackageState,
)
from distributed_alignment.scheduler.filesystem_backend import (
    FileSystemWorkStack,
)

if TYPE_CHECKING:
    from pathlib import Path


def _make_manifests(
    num_query_chunks: int, num_ref_chunks: int
) -> tuple[ChunkManifest, ChunkManifest]:
    """Create minimal ChunkManifest objects for testing."""
    now = datetime.now(tz=UTC)
    query_manifest = ChunkManifest(
        run_id="test_run",
        input_files=["queries.fasta"],
        total_sequences=num_query_chunks * 100,
        num_chunks=num_query_chunks,
        chunk_size_target=100,
        chunks=[
            ChunkEntry(
                chunk_id=f"q{i:03d}",
                num_sequences=100,
                parquet_path=f"chunks/queries/chunk_q{i:03d}.parquet",
                content_checksum=f"sha256:fake_{i}",
            )
            for i in range(num_query_chunks)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    ref_manifest = ChunkManifest(
        run_id="test_run",
        input_files=["reference.fasta"],
        total_sequences=num_ref_chunks * 1000,
        num_chunks=num_ref_chunks,
        chunk_size_target=1000,
        chunks=[
            ChunkEntry(
                chunk_id=f"r{i:03d}",
                num_sequences=1000,
                parquet_path=f"chunks/refs/chunk_r{i:03d}.parquet",
                content_checksum=f"sha256:fake_ref_{i}",
            )
            for i in range(num_ref_chunks)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    return query_manifest, ref_manifest


class TestGenerateWorkPackages:
    """Tests for work package generation."""

    def test_cartesian_product(self, tmp_path: Path) -> None:
        """3 query × 2 ref → 6 packages."""
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(3, 2)

        packages = stack.generate_work_packages(q_manifest, r_manifest)

        assert len(packages) == 6
        assert stack.pending_count() == 6

    def test_all_packages_are_pending(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 2)

        packages = stack.generate_work_packages(q_manifest, r_manifest)

        for pkg in packages:
            assert pkg.state == WorkPackageState.PENDING

    def test_package_ids_are_correct(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 3)

        packages = stack.generate_work_packages(q_manifest, r_manifest)

        expected_ids = {f"wp_q{q:03d}_r{r:03d}" for q in range(2) for r in range(3)}
        actual_ids = {p.package_id for p in packages}
        assert actual_ids == expected_ids

    def test_json_files_written_to_pending(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 2)

        stack.generate_work_packages(q_manifest, r_manifest)

        pending_files = list((tmp_path / "pending").glob("*.json"))
        assert len(pending_files) == 4

    def test_max_attempts_propagated(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)

        packages = stack.generate_work_packages(q_manifest, r_manifest, max_attempts=5)

        assert packages[0].max_attempts == 5


class TestClaim:
    """Tests for atomic claim mechanism."""

    def test_claim_returns_package(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 2)
        stack.generate_work_packages(q_manifest, r_manifest)

        package = stack.claim("worker-1")

        assert package is not None
        assert package.state == WorkPackageState.RUNNING
        assert package.claimed_by == "worker-1"
        assert package.claimed_at is not None
        assert package.started_at is not None
        assert package.heartbeat_at is not None

    def test_claim_decrements_pending(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        assert stack.pending_count() == 2
        stack.claim("worker-1")
        assert stack.pending_count() == 1

    def test_claim_increments_running(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        assert stack.status()["RUNNING"] == 0
        stack.claim("worker-1")
        assert stack.status()["RUNNING"] == 1

    def test_claim_from_empty_returns_none(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)

        result = stack.claim("worker-1")

        assert result is None

    def test_claim_all_packages(self, tmp_path: Path) -> None:
        """Claim all packages one by one, then next claim returns None."""
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(2, 2)
        stack.generate_work_packages(q_manifest, r_manifest)

        claimed = []
        for _ in range(4):
            pkg = stack.claim("worker-1")
            assert pkg is not None
            claimed.append(pkg.package_id)

        assert stack.claim("worker-1") is None
        assert len(set(claimed)) == 4  # All unique


class TestComplete:
    """Tests for package completion."""

    def test_complete_moves_to_completed(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        package = stack.claim("worker-1")
        assert package is not None

        stack.complete(package.package_id, "/results/output.parquet")

        status = stack.status()
        assert status["RUNNING"] == 0
        assert status["COMPLETED"] == 1

    def test_completed_at_is_set(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        package = stack.claim("worker-1")
        assert package is not None
        stack.complete(package.package_id, "/results/output.parquet")

        # Read the completed package JSON
        completed_path = tmp_path / "completed" / f"{package.package_id}.json"
        data = json.loads(completed_path.read_text())
        assert data["completed_at"] is not None
        assert data["state"] == "COMPLETED"


class TestFail:
    """Tests for package failure and retry logic."""

    def test_fail_with_retries_remaining(self, tmp_path: Path) -> None:
        """attempt < max_attempts → back to PENDING."""
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest, max_attempts=3)

        package = stack.claim("worker-1")
        assert package is not None
        stack.fail(package.package_id, "diamond_exit_code_137_oom")

        status = stack.status()
        assert status["RUNNING"] == 0
        assert status["PENDING"] == 1
        assert status["POISONED"] == 0

    def test_fail_increments_attempt(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        package = stack.claim("worker-1")
        assert package is not None
        stack.fail(package.package_id, "error_1")

        # Re-claim and check attempt count
        retried = stack.claim("worker-2")
        assert retried is not None
        assert retried.attempt == 1
        assert retried.error_history == ["error_1"]

    def test_fail_appends_to_error_history(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest, max_attempts=3)

        # Fail twice, reclaiming in between
        pkg = stack.claim("worker-1")
        assert pkg is not None
        stack.fail(pkg.package_id, "error_1")

        pkg = stack.claim("worker-2")
        assert pkg is not None
        stack.fail(pkg.package_id, "error_2")

        # Read the pending package
        pending_path = tmp_path / "pending" / f"{pkg.package_id}.json"
        data = json.loads(pending_path.read_text())
        assert data["error_history"] == ["error_1", "error_2"]
        assert data["attempt"] == 2

    def test_fail_max_attempts_exhausted_goes_to_poisoned(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest, max_attempts=2)

        # Fail twice (max_attempts=2)
        pkg = stack.claim("worker-1")
        assert pkg is not None
        stack.fail(pkg.package_id, "error_1")

        pkg = stack.claim("worker-2")
        assert pkg is not None
        stack.fail(pkg.package_id, "error_2")

        status = stack.status()
        assert status["PENDING"] == 0
        assert status["RUNNING"] == 0
        assert status["POISONED"] == 1

    def test_fail_clears_worker_fields(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        pkg = stack.claim("worker-1")
        assert pkg is not None
        stack.fail(pkg.package_id, "error")

        retried = stack.claim("worker-2")
        assert retried is not None
        assert retried.claimed_by == "worker-2"  # New worker, not old


class TestHeartbeat:
    """Tests for heartbeat updates."""

    def test_heartbeat_updates_timestamp(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        pkg = stack.claim("worker-1")
        assert pkg is not None
        original_hb = pkg.heartbeat_at

        stack.heartbeat(pkg.package_id)

        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        new_hb = datetime.fromisoformat(data["heartbeat_at"])
        assert new_hb >= original_hb  # type: ignore[operator]


class TestReapStale:
    """Tests for stale heartbeat reaping."""

    def test_reap_stale_package(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        # Manually set heartbeat to the past
        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        old_time = datetime.now(tz=UTC) - timedelta(seconds=300)
        data["heartbeat_at"] = old_time.isoformat()
        path.write_text(json.dumps(data, indent=2))

        reaped = stack.reap_stale(timeout_seconds=60)

        assert len(reaped) == 1
        assert reaped[0] == pkg.package_id
        assert stack.status()["RUNNING"] == 0
        assert stack.status()["PENDING"] == 1

    def test_reap_does_not_touch_fresh_packages(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        stack.claim("worker-1")

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == []
        assert stack.status()["RUNNING"] == 1

    def test_reap_exhausted_goes_to_poisoned(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest, max_attempts=1)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        # Set stale heartbeat
        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        old_time = datetime.now(tz=UTC) - timedelta(seconds=300)
        data["heartbeat_at"] = old_time.isoformat()
        path.write_text(json.dumps(data, indent=2))

        reaped = stack.reap_stale(timeout_seconds=60)

        assert len(reaped) == 1
        assert stack.status()["POISONED"] == 1
        assert stack.status()["PENDING"] == 0


class TestStatus:
    """Tests for status reporting."""

    def test_status_matches_directory_contents(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(3, 2)
        stack.generate_work_packages(q_manifest, r_manifest)

        # Claim 2, complete 1
        pkg1 = stack.claim("worker-1")
        assert pkg1 is not None
        pkg2 = stack.claim("worker-2")
        assert pkg2 is not None
        stack.complete(pkg1.package_id, "/results/1.parquet")

        status = stack.status()
        assert status["PENDING"] == 4
        assert status["RUNNING"] == 1
        assert status["COMPLETED"] == 1
        assert status["POISONED"] == 0

    def test_status_all_states(self, tmp_path: Path) -> None:
        """Verify all expected state keys are present."""
        stack = FileSystemWorkStack(tmp_path)
        status = stack.status()
        assert set(status.keys()) == {
            "PENDING",
            "RUNNING",
            "COMPLETED",
            "POISONED",
        }


class TestConcurrentClaims:
    """Tests for atomic claim semantics under concurrency."""

    def test_no_duplicate_claims(self, tmp_path: Path) -> None:
        """10 threads claiming from 5 packages → each claimed exactly once."""
        stack = FileSystemWorkStack(tmp_path)
        q_manifest, r_manifest = _make_manifests(5, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        claimed: list[str] = []
        lock = threading.Lock()
        errors: list[Exception] = []

        def worker(worker_id: str) -> None:
            try:
                while True:
                    pkg = stack.claim(worker_id)
                    if pkg is None:
                        break
                    with lock:
                        claimed.append(pkg.package_id)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors during concurrent claims: {errors}"
        assert len(claimed) == 5, f"Expected 5 claims, got {len(claimed)}"
        assert len(set(claimed)) == 5, f"Duplicate claims: {claimed}"
        assert stack.pending_count() == 0
        assert stack.status()["RUNNING"] == 5


class TestDirectoryInit:
    """Tests for directory initialization."""

    def test_creates_state_directories(self, tmp_path: Path) -> None:
        base = tmp_path / "fresh"
        FileSystemWorkStack(base)

        assert (base / "pending").is_dir()
        assert (base / "running").is_dir()
        assert (base / "completed").is_dir()
        assert (base / "poisoned").is_dir()

    def test_idempotent_init(self, tmp_path: Path) -> None:
        """Creating a stack twice on the same dir doesn't error."""
        FileSystemWorkStack(tmp_path)
        FileSystemWorkStack(tmp_path)  # Should not raise
