"""Tests for the timeout reaper mechanism."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from distributed_alignment.models import (
    ChunkEntry,
    ChunkManifest,
    ProteinSequence,
)
from distributed_alignment.scheduler.filesystem_backend import (
    FileSystemWorkStack,
)
from distributed_alignment.worker.diamond_wrapper import (
    DiamondResult,
    DiamondWrapper,
)
from distributed_alignment.worker.runner import ReaperThread, WorkerRunner

if TYPE_CHECKING:
    from pathlib import Path


def _make_manifests(
    num_q: int = 1, num_r: int = 1
) -> tuple[ChunkManifest, ChunkManifest]:
    now = datetime.now(tz=UTC)
    q = ChunkManifest(
        run_id="test",
        input_files=["q.fasta"],
        total_sequences=num_q * 10,
        num_chunks=num_q,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id=f"q{i:03d}",
                num_sequences=10,
                parquet_path=f"chunks/queries/chunk_q{i:03d}.parquet",
                content_checksum="sha256:fake",
            )
            for i in range(num_q)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    r = ChunkManifest(
        run_id="test",
        input_files=["r.fasta"],
        total_sequences=num_r * 10,
        num_chunks=num_r,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id=f"r{i:03d}",
                num_sequences=10,
                parquet_path=f"chunks/refs/chunk_r{i:03d}.parquet",
                content_checksum="sha256:fake",
            )
            for i in range(num_r)
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    return q, r


def _make_package_stale(tmp_path: Path, package_id: str, age_seconds: float) -> None:
    """Set a running package's heartbeat_at to the past."""
    path = tmp_path / "running" / f"{package_id}.json"
    data = json.loads(path.read_text())
    old_time = datetime.now(tz=UTC) - timedelta(seconds=age_seconds)
    data["heartbeat_at"] = old_time.isoformat()
    path.write_text(json.dumps(data, indent=2))


class TestReapStaleBasics:
    """Tests for FileSystemWorkStack.reap_stale()."""

    def test_stale_package_moved_to_pending(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-dead")
        assert pkg is not None
        _make_package_stale(tmp_path, pkg.package_id, 300)

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == [pkg.package_id]
        assert stack.status()["RUNNING"] == 0
        assert stack.status()["PENDING"] == 1

    def test_stale_package_attempt_incremented(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-dead")
        assert pkg is not None
        _make_package_stale(tmp_path, pkg.package_id, 300)

        stack.reap_stale(timeout_seconds=60)

        # Reclaim and check attempt was incremented
        reclaimed = stack.claim("worker-new")
        assert reclaimed is not None
        assert reclaimed.attempt == 1

    def test_stale_package_has_error_history(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-dead")
        assert pkg is not None
        _make_package_stale(tmp_path, pkg.package_id, 300)

        stack.reap_stale(timeout_seconds=60)

        reclaimed = stack.claim("worker-new")
        assert reclaimed is not None
        assert len(reclaimed.error_history) == 1
        assert "heartbeat stale" in reclaimed.error_history[0]

    def test_fresh_package_left_alone(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-alive")
        assert pkg is not None
        # Heartbeat is set to now by claim — it's fresh

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == []
        assert stack.status()["RUNNING"] == 1

    def test_null_heartbeat_treated_as_stale(self, tmp_path: Path) -> None:
        """Package with heartbeat_at=None is considered stale."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-dead")
        assert pkg is not None

        # Set heartbeat_at to null
        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        data["heartbeat_at"] = None
        path.write_text(json.dumps(data, indent=2))

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == [pkg.package_id]
        assert stack.status()["PENDING"] == 1

    def test_max_attempts_exhausted_goes_to_poisoned(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r, max_attempts=1)

        pkg = stack.claim("worker-dead")
        assert pkg is not None
        _make_package_stale(tmp_path, pkg.package_id, 300)

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == [pkg.package_id]
        assert stack.status()["POISONED"] == 1
        assert stack.status()["PENDING"] == 0

    def test_empty_running_directory(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)

        reaped = stack.reap_stale(timeout_seconds=60)

        assert reaped == []

    def test_multiple_stale_packages(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests(num_q=3, num_r=1)
        stack.generate_work_packages(q, r)

        # Claim all 3 and make them stale
        for _ in range(3):
            pkg = stack.claim("worker-dead")
            assert pkg is not None
            _make_package_stale(tmp_path, pkg.package_id, 300)

        reaped = stack.reap_stale(timeout_seconds=60)

        assert len(reaped) == 3
        assert stack.status()["RUNNING"] == 0
        assert stack.status()["PENDING"] == 3


class TestReapStaleRaceConditions:
    """Tests for race condition handling in reap_stale."""

    def test_file_disappears_between_listing_and_reading(self, tmp_path: Path) -> None:
        """Simulate a worker completing a package during reap scan."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests(num_q=2, num_r=1)
        stack.generate_work_packages(q, r)

        pkg1 = stack.claim("worker-1")
        pkg2 = stack.claim("worker-2")
        assert pkg1 is not None and pkg2 is not None

        _make_package_stale(tmp_path, pkg1.package_id, 300)
        _make_package_stale(tmp_path, pkg2.package_id, 300)

        # Complete pkg2 before reap runs (simulates race)
        stack.complete(pkg2.package_id, "/fake/result.parquet")

        reaped = stack.reap_stale(timeout_seconds=60)

        # Only pkg1 should be reaped; pkg2 was already completed
        assert pkg1.package_id in reaped
        assert pkg2.package_id not in reaped


class TestReaperThread:
    """Tests for the ReaperThread context manager."""

    def test_reaper_detects_stale_package(self, tmp_path: Path) -> None:
        """With short interval, reaper catches stale packages."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-dead")
        assert pkg is not None
        _make_package_stale(tmp_path, pkg.package_id, 300)

        with ReaperThread(stack, timeout_seconds=1, interval=0.05):
            # Give the reaper a few cycles to detect the stale package
            time.sleep(0.3)

        assert stack.status()["RUNNING"] == 0
        assert stack.status()["PENDING"] == 1

    def test_stops_cleanly_on_context_exit(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)

        reaper: ReaperThread
        with ReaperThread(stack, timeout_seconds=60, interval=0.05) as reaper:
            time.sleep(0.1)
            assert reaper.is_alive

        assert not reaper.is_alive

    def test_handles_exception_in_reap_stale(self) -> None:
        mock_stack = MagicMock(spec=FileSystemWorkStack)
        mock_stack.reap_stale.side_effect = RuntimeError("test")

        with ReaperThread(
            mock_stack,  # type: ignore[arg-type]
            timeout_seconds=60,
            interval=0.05,
        ) as reaper:
            time.sleep(0.2)

        # Thread should still have been alive during the context
        # (warning logged, not crashed)
        assert not reaper.is_alive


class TestWorkerRunnerWithReaper:
    """Integration: reaper reclaims a dead worker's package."""

    def test_reaper_reclaims_dead_worker_package(self, tmp_path: Path) -> None:
        """Worker A claims and dies. Worker B's reaper reclaims it."""
        from distributed_alignment.ingest.chunker import chunk_sequences

        # Create chunk files
        chunks_dir = tmp_path / "chunks"
        amino = "ACDEFGHIKLMNPQRSTVWY"
        sequences = [
            ProteinSequence(
                id=f"seq_{i:04d}",
                description=f"seq_{i:04d} test",
                sequence=amino * 3,
                length=len(amino) * 3,
            )
            for i in range(5)
        ]
        chunk_sequences(
            sequences,
            num_chunks=1,
            output_dir=chunks_dir / "queries",
            chunk_prefix="q",
            run_id="test",
            input_files=["q.fasta"],
        )
        chunk_sequences(
            sequences,
            num_chunks=1,
            output_dir=chunks_dir / "refs",
            chunk_prefix="r",
            run_id="test",
            input_files=["r.fasta"],
        )

        # Set up work stack with 1 package
        stack = FileSystemWorkStack(tmp_path / "work")
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        # Worker A claims the package and "dies" (stale heartbeat)
        pkg = stack.claim("worker-A-dead")
        assert pkg is not None
        _make_package_stale(tmp_path / "work", pkg.package_id, 300)
        assert stack.pending_count() == 0
        assert stack.status()["RUNNING"] == 1

        # Worker B starts with reaper — should reclaim and process
        mock_diamond = MagicMock(spec=DiamondWrapper)
        mock_diamond.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )

        def fake_blastp(
            query_fasta: Path,
            ref_db: Path,
            output_path: Path,
            **kwargs: object,
        ) -> DiamondResult:
            output_path.write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0,
                duration_seconds=0.1,
                stderr="",
                output_path=str(output_path),
            )

        mock_diamond.run_blastp.side_effect = fake_blastp

        # Step 1: Run the reaper to reclaim the stale package.
        # In production, the reaper runs inside the worker loop, but
        # the worker loop exits immediately if nothing is pending.
        # So we run the reaper first, then the worker.
        with ReaperThread(stack, timeout_seconds=1, interval=0.05):
            # Wait for the reaper to detect and reclaim
            for _ in range(20):
                if stack.pending_count() > 0:
                    break
                time.sleep(0.05)

        assert stack.pending_count() == 1, (
            "Reaper should have reclaimed the stale package"
        )

        # Step 2: Now run the worker — it should claim and process
        runner = WorkerRunner(
            stack,
            mock_diamond,  # type: ignore[arg-type]
            chunks_dir,
            tmp_path / "results",
            heartbeat_interval=0.05,
            heartbeat_timeout=1,
            reaper_interval=0.05,
        )
        completed = runner.run()

        assert completed == 1
        assert stack.status()["COMPLETED"] == 1
        assert stack.status()["RUNNING"] == 0
        assert stack.pending_count() == 0
