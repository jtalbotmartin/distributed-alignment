"""Tests for the heartbeat mechanism."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from distributed_alignment.models import (
    ChunkEntry,
    ChunkManifest,
)
from distributed_alignment.scheduler.filesystem_backend import (
    FileSystemWorkStack,
)
from distributed_alignment.worker.diamond_wrapper import (
    DiamondResult,
    DiamondWrapper,
)
from distributed_alignment.worker.runner import HeartbeatSender, WorkerRunner

if TYPE_CHECKING:
    from pathlib import Path


def _make_manifests() -> tuple[ChunkManifest, ChunkManifest]:
    now = datetime.now(tz=UTC)
    q = ChunkManifest(
        run_id="test",
        input_files=["q.fasta"],
        total_sequences=10,
        num_chunks=1,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id="q000",
                num_sequences=10,
                parquet_path="chunks/queries/chunk_q000.parquet",
                content_checksum="sha256:fake",
            )
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    r = ChunkManifest(
        run_id="test",
        input_files=["r.fasta"],
        total_sequences=10,
        num_chunks=1,
        chunk_size_target=10,
        chunks=[
            ChunkEntry(
                chunk_id="r000",
                num_sequences=10,
                parquet_path="chunks/refs/chunk_r000.parquet",
                content_checksum="sha256:fake",
            )
        ],
        created_at=now,
        chunking_strategy="deterministic_hash",
    )
    return q, r


class TestHeartbeatMethod:
    """Tests for FileSystemWorkStack.heartbeat()."""

    def test_heartbeat_updates_timestamp(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-1")
        assert pkg is not None
        original_hb = pkg.heartbeat_at

        time.sleep(0.01)
        stack.heartbeat(pkg.package_id)

        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        new_hb = datetime.fromisoformat(data["heartbeat_at"])
        assert new_hb > original_hb  # type: ignore[operator]

    def test_heartbeat_on_moved_package_does_not_crash(self, tmp_path: Path) -> None:
        """If the package was completed/failed, heartbeat is a no-op."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        # Complete the package — moves it out of running/
        stack.complete(pkg.package_id, "/results/fake.parquet")

        # Heartbeat should not raise
        stack.heartbeat(pkg.package_id)  # no error


class TestHeartbeatSender:
    """Tests for the HeartbeatSender context manager."""

    def test_sends_periodic_heartbeats(self, tmp_path: Path) -> None:
        """With short interval, heartbeat fires multiple times."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        with HeartbeatSender(stack, pkg.package_id, interval=0.05):
            time.sleep(0.3)

        # Read the final heartbeat_at
        path = tmp_path / "running" / f"{pkg.package_id}.json"
        data = json.loads(path.read_text())
        hb = datetime.fromisoformat(data["heartbeat_at"])
        # Should be more recent than claimed_at
        claimed = datetime.fromisoformat(data["claimed_at"])
        assert hb > claimed

    def test_stops_cleanly_on_context_exit(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        sender: HeartbeatSender
        with HeartbeatSender(stack, pkg.package_id, interval=0.05) as sender:
            time.sleep(0.1)
            assert sender.is_alive

        # After exiting context, thread should be stopped
        assert not sender.is_alive

    def test_handles_package_completion_gracefully(self, tmp_path: Path) -> None:
        """If the package is moved while heartbeat is running, no crash."""
        stack = FileSystemWorkStack(tmp_path)
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        pkg = stack.claim("worker-1")
        assert pkg is not None

        with HeartbeatSender(stack, pkg.package_id, interval=0.05):
            time.sleep(0.1)
            # Complete the package mid-heartbeat
            stack.complete(pkg.package_id, "/results/fake.parquet")
            time.sleep(0.15)
            # Heartbeat should have handled the FileNotFoundError

        # Should complete without error
        assert stack.status()["COMPLETED"] == 1

    def test_handles_heartbeat_exception(self, tmp_path: Path) -> None:
        """If heartbeat() raises, the thread stops but doesn't crash."""
        mock_stack = MagicMock(spec=FileSystemWorkStack)
        mock_stack.heartbeat.side_effect = RuntimeError("test error")

        with HeartbeatSender(
            mock_stack,  # type: ignore[arg-type]
            "wp_q000_r000",
            interval=0.05,
        ) as sender:
            time.sleep(0.15)

        # Thread should have stopped after the error
        assert not sender.is_alive


class TestWorkerRunnerWithHeartbeat:
    """Tests that the worker runner uses heartbeats during processing."""

    def test_heartbeat_fires_during_processing(self, tmp_path: Path) -> None:
        """With a slow mock DIAMOND, heartbeat should update during work."""
        from distributed_alignment.ingest.chunker import chunk_sequences
        from distributed_alignment.models import ProteinSequence

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

        # Set up work stack
        stack = FileSystemWorkStack(tmp_path / "work")
        q, r = _make_manifests()
        stack.generate_work_packages(q, r)

        # Mock DIAMOND with a slow blastp (0.3s delay)
        mock_diamond = MagicMock(spec=DiamondWrapper)
        mock_diamond.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )

        def slow_blastp(
            query_fasta: Path,
            ref_db: Path,
            output_path: Path,
            **kwargs: object,
        ) -> DiamondResult:
            time.sleep(0.3)
            output_path.write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0,
                duration_seconds=0.3,
                stderr="",
                output_path=str(output_path),
            )

        mock_diamond.run_blastp.side_effect = slow_blastp

        # Run with very short heartbeat interval
        results_dir = tmp_path / "results"
        runner = WorkerRunner(
            stack,
            mock_diamond,  # type: ignore[arg-type]
            chunks_dir,
            results_dir,
            heartbeat_interval=0.05,
        )
        completed = runner.run()

        assert completed == 1

        # Read the completed package and check heartbeat was updated
        completed_dir = tmp_path / "work" / "completed"
        completed_files = list(completed_dir.glob("*.json"))
        assert len(completed_files) == 1

        data = json.loads(completed_files[0].read_text())
        assert data["heartbeat_at"] is not None
