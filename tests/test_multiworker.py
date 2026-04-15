"""Tests for multi-worker execution via multiprocessing."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from distributed_alignment.ingest.chunker import chunk_sequences
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
from distributed_alignment.worker.runner import WorkerRunner

if TYPE_CHECKING:
    from pathlib import Path

AMINO = "ACDEFGHIKLMNPQRSTVWY"


def _slow_worker_target(work_dir: str) -> None:
    """Target for multiprocessing.Process — claims a package and sleeps.

    Module-level so it's picklable (required for macOS spawn start method).
    """
    from pathlib import Path

    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )

    s = FileSystemWorkStack(Path(work_dir))
    pkg = s.claim("slow-worker")
    if pkg is not None:
        s.heartbeat(pkg.package_id)
        time.sleep(60)  # Will be killed before this finishes


def _make_sequences(n: int) -> list[ProteinSequence]:
    return [
        ProteinSequence(
            id=f"seq_{i:04d}",
            description=f"seq_{i:04d} test",
            sequence=AMINO * 3,
            length=len(AMINO) * 3,
        )
        for i in range(n)
    ]


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


def _make_mock_diamond(*, delay: float = 0.0) -> DiamondWrapper:
    """Create a mock DiamondWrapper with optional processing delay."""
    mock = MagicMock(spec=DiamondWrapper)
    mock.make_db.return_value = DiamondResult(
        exit_code=0, duration_seconds=0.01, stderr=""
    )

    def fake_blastp(
        query_fasta: Path,
        ref_db: Path,
        output_path: Path,
        **kwargs: object,
    ) -> DiamondResult:
        if delay > 0:
            time.sleep(delay)
        output_path.write_text(
            "seq_0001\tref_0001\t85.0\t100\t15\t0\t1\t100\t1\t100\t1e-30\t200.0\n"
        )
        return DiamondResult(
            exit_code=0,
            duration_seconds=delay,
            stderr="",
            output_path=str(output_path),
        )

    mock.run_blastp.side_effect = fake_blastp
    return mock  # type: ignore[return-value]


def _setup_work(
    tmp_path: Path, num_q: int = 2, num_r: int = 2
) -> tuple[FileSystemWorkStack, Path, Path]:
    """Create chunks, manifests, and work packages."""
    chunks_dir = tmp_path / "chunks"
    seqs = _make_sequences(10)

    chunk_sequences(
        seqs,
        num_chunks=num_q,
        output_dir=chunks_dir / "queries",
        chunk_prefix="q",
        run_id="test",
        input_files=["q.fasta"],
    )
    chunk_sequences(
        seqs,
        num_chunks=num_r,
        output_dir=chunks_dir / "refs",
        chunk_prefix="r",
        run_id="test",
        input_files=["r.fasta"],
    )

    stack = FileSystemWorkStack(tmp_path / "work")
    q, r = _make_manifests(num_q, num_r)
    stack.generate_work_packages(q, r)

    return stack, chunks_dir, tmp_path / "results"


class TestPollingLoop:
    """Tests for the polling claim loop with backoff."""

    def test_worker_exits_on_idle(self, tmp_path: Path) -> None:
        """Worker with no packages exits after max_idle_time."""
        stack = FileSystemWorkStack(tmp_path / "work")
        diamond = _make_mock_diamond()

        runner = WorkerRunner(
            stack,
            diamond,
            tmp_path / "chunks",
            tmp_path / "results",
            max_idle_time=1.0,
        )

        start = time.monotonic()
        completed = runner.run()
        elapsed = time.monotonic() - start

        assert completed == 0
        # Should have waited ~1s before exiting
        assert 0.8 <= elapsed <= 3.0

    def test_worker_processes_and_exits(self, tmp_path: Path) -> None:
        """Worker processes available packages then exits on idle."""
        stack, chunks_dir, results_dir = _setup_work(tmp_path, num_q=1, num_r=1)
        diamond = _make_mock_diamond(delay=0.05)

        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            max_idle_time=1.0,
        )

        completed = runner.run()

        assert completed == 1
        assert stack.status()["COMPLETED"] == 1

    def test_shutdown_signal_stops_worker(self, tmp_path: Path) -> None:
        """request_shutdown() stops the worker mid-poll."""
        stack = FileSystemWorkStack(tmp_path / "work")
        diamond = _make_mock_diamond()

        runner = WorkerRunner(
            stack,
            diamond,
            tmp_path / "chunks",
            tmp_path / "results",
            max_idle_time=30.0,
        )

        import threading

        # Signal shutdown after a short delay
        def shutdown_after() -> None:
            time.sleep(0.5)
            runner.request_shutdown()

        t = threading.Thread(target=shutdown_after)
        t.start()

        start = time.monotonic()
        runner.run()
        elapsed = time.monotonic() - start

        t.join()
        # Should have exited ~0.5s (from shutdown), not 30s (from idle)
        assert elapsed < 5.0


class TestMultiWorker:
    """Tests for multiple workers processing concurrently."""

    def test_all_packages_completed(self, tmp_path: Path) -> None:
        """4 workers processing 8 packages → all 8 completed."""
        stack, chunks_dir, results_dir = _setup_work(tmp_path, num_q=4, num_r=2)

        assert stack.pending_count() == 8

        # Run 4 workers sequentially (simulates concurrent via
        # shared filesystem atomics — each worker claims different
        # packages)
        total_completed = 0
        for _ in range(4):
            diamond = _make_mock_diamond(delay=0.02)
            runner = WorkerRunner(
                stack,
                diamond,
                chunks_dir,
                results_dir,
                max_idle_time=0.5,
            )
            total_completed += runner.run()

        assert total_completed == 8
        assert stack.status()["COMPLETED"] == 8
        assert stack.status()["POISONED"] == 0

    def test_no_duplicate_claims(self, tmp_path: Path) -> None:
        """Each package_id appears exactly once in completed/."""
        stack, chunks_dir, results_dir = _setup_work(tmp_path, num_q=3, num_r=2)

        for _ in range(3):
            diamond = _make_mock_diamond()
            runner = WorkerRunner(
                stack,
                diamond,
                chunks_dir,
                results_dir,
                max_idle_time=0.5,
            )
            runner.run()

        completed_dir = tmp_path / "work" / "completed"
        completed_ids = [
            json.loads(f.read_text())["package_id"]
            for f in completed_dir.glob("*.json")
        ]
        assert len(completed_ids) == 6
        assert len(set(completed_ids)) == 6  # All unique


class TestWorkerDeathRecovery:
    """Tests for fault tolerance: dead worker → reaper → recovery."""

    def test_dead_worker_package_recovered(self, tmp_path: Path) -> None:
        """Worker A dies. Worker B's reaper reclaims the package."""
        stack, chunks_dir, results_dir = _setup_work(tmp_path, num_q=1, num_r=1)
        work_dir = tmp_path / "work"

        # Worker A claims and "dies" (stale heartbeat)
        pkg = stack.claim("worker-A-dead")
        assert pkg is not None
        path = work_dir / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        old = datetime.now(tz=UTC) - timedelta(seconds=300)
        data["heartbeat_at"] = old.isoformat()
        path.write_text(json.dumps(data, indent=2))

        assert stack.pending_count() == 0

        # Worker B with aggressive reaper reclaims it
        diamond = _make_mock_diamond()
        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            heartbeat_interval=0.05,
            heartbeat_timeout=1,
            reaper_interval=0.1,
            max_idle_time=3.0,
        )
        completed = runner.run()

        assert completed == 1
        assert stack.status()["COMPLETED"] == 1


class TestMultiprocessWorkerDeath:
    """Test actual process death and recovery via multiprocessing.

    Uses real processes and SIGKILL for realistic fault tolerance
    testing. Needs generous timeouts to avoid flakiness.
    """

    def test_killed_worker_package_recovered(self, tmp_path: Path) -> None:
        """SIGKILL a worker process, verify another recovers its work."""
        stack, chunks_dir, results_dir = _setup_work(tmp_path, num_q=2, num_r=1)
        work_dir = tmp_path / "work"

        # Start the slow worker and let it claim a package
        slow_proc = multiprocessing.Process(
            target=_slow_worker_target,
            args=(str(work_dir),),
        )
        slow_proc.start()
        time.sleep(0.3)  # Let it claim

        # Verify it claimed something
        assert stack.status()["RUNNING"] >= 1

        # Kill the slow worker
        os.kill(slow_proc.pid, signal.SIGKILL)
        slow_proc.join(timeout=2)

        # Now make its heartbeat stale
        for path in (work_dir / "running").glob("*.json"):
            data = json.loads(path.read_text())
            if data.get("claimed_by") == "slow-worker":
                old = datetime.now(tz=UTC) - timedelta(seconds=300)
                data["heartbeat_at"] = old.isoformat()
                path.write_text(json.dumps(data, indent=2))

        # Recovery worker with reaper
        diamond = _make_mock_diamond()
        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            heartbeat_interval=0.05,
            heartbeat_timeout=1,
            reaper_interval=0.1,
            max_idle_time=3.0,
        )
        completed = runner.run()

        # Should have processed both packages (1 reclaimed + 1 fresh)
        assert completed == 2
        assert stack.status()["COMPLETED"] == 2
        assert stack.status()["POISONED"] == 0
