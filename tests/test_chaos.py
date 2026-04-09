"""Chaos tests — verifying fault tolerance under failure scenarios.

These tests intentionally break things: kill processes, corrupt data,
simulate disk errors, and inject DIAMOND failures. Each test verifies
that the system recovers correctly.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

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

AMINO = "ACDEFGHIKLMNPQRSTVWY"


# --- Module-level multiprocessing targets (required for macOS spawn) ---


def _slow_worker_target(
    work_dir: str,
    chunks_dir: str,
    results_dir: str,
    delay: float,
) -> None:
    """Worker process that sleeps during DIAMOND (simulating long alignment)."""
    from distributed_alignment.scheduler.filesystem_backend import (
        FileSystemWorkStack,
    )
    from distributed_alignment.worker.diamond_wrapper import (
        DiamondResult,
        DiamondWrapper,
    )
    from distributed_alignment.worker.runner import WorkerRunner

    mock = MagicMock(spec=DiamondWrapper)
    mock.make_db.return_value = DiamondResult(
        exit_code=0, duration_seconds=0.01, stderr=""
    )

    def fake_blastp(
        query_fasta: object,
        ref_db: object,
        output_path: Path,
        **kwargs: object,
    ) -> DiamondResult:
        time.sleep(delay)
        Path(str(output_path)).write_text(
            "seq_0001\tref_0001\t85.0\t100\t15\t0"
            "\t1\t100\t1\t100\t1e-30\t200.0\n"
        )
        return DiamondResult(
            exit_code=0, duration_seconds=delay, stderr="",
            output_path=str(output_path),
        )

    mock.run_blastp.side_effect = fake_blastp

    stack = FileSystemWorkStack(Path(work_dir))
    runner = WorkerRunner(
        stack,
        mock,
        Path(chunks_dir),
        Path(results_dir),
        heartbeat_interval=0.2,
        heartbeat_timeout=2,
        reaper_interval=0.5,
        max_idle_time=5.0,
    )
    runner.run()


# --- Helpers ---


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
    num_q: int = 2, num_r: int = 1
) -> tuple[ChunkManifest, ChunkManifest]:
    now = datetime.now(tz=UTC)
    q = ChunkManifest(
        run_id="chaos_test",
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
        run_id="chaos_test",
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


def _setup_work(
    tmp_path: Path, num_q: int = 2, num_r: int = 1
) -> tuple[FileSystemWorkStack, Path, Path]:
    """Create chunks, manifests, and work packages."""
    chunks_dir = tmp_path / "chunks"
    seqs = _make_sequences(10)
    chunk_sequences(
        seqs,
        num_chunks=num_q,
        output_dir=chunks_dir / "queries",
        chunk_prefix="q",
        run_id="chaos_test",
        input_files=["q.fasta"],
    )
    chunk_sequences(
        seqs,
        num_chunks=num_r,
        output_dir=chunks_dir / "refs",
        chunk_prefix="r",
        run_id="chaos_test",
        input_files=["r.fasta"],
    )
    stack = FileSystemWorkStack(tmp_path / "work")
    q, r = _make_manifests(num_q, num_r)
    stack.generate_work_packages(q, r)
    return stack, chunks_dir, tmp_path / "results"


def _make_mock_diamond(
    *, delay: float = 0.0
) -> DiamondWrapper:
    mock = MagicMock(spec=DiamondWrapper)
    mock.make_db.return_value = DiamondResult(
        exit_code=0, duration_seconds=0.01, stderr=""
    )

    def fake_blastp(
        query_fasta: object,
        ref_db: object,
        output_path: Path,
        **kwargs: object,
    ) -> DiamondResult:
        if delay > 0:
            time.sleep(delay)
        Path(str(output_path)).write_text(
            "seq_0001\tref_0001\t85.0\t100\t15\t0"
            "\t1\t100\t1\t100\t1e-30\t200.0\n"
        )
        return DiamondResult(
            exit_code=0, duration_seconds=delay, stderr="",
            output_path=str(output_path),
        )

    mock.run_blastp.side_effect = fake_blastp
    return mock  # type: ignore[return-value]


def _make_stale(work_dir: Path, package_id: str) -> None:
    path = work_dir / "running" / f"{package_id}.json"
    data = json.loads(path.read_text())
    old = datetime.now(tz=UTC) - timedelta(seconds=300)
    data["heartbeat_at"] = old.isoformat()
    path.write_text(json.dumps(data, indent=2))


def _wait_for(
    stack: FileSystemWorkStack,
    *,
    completed: int | None = None,
    timeout: float = 30.0,
) -> None:
    """Poll until the expected state is reached or timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = stack.status()
        if completed is not None and s.get("COMPLETED", 0) >= completed:
            return
        time.sleep(0.2)
    s = stack.status()
    msg = f"Timed out waiting for state. Current: {s}"
    raise TimeoutError(msg)


# === Multiprocessing chaos tests ===


@pytest.mark.integration
class TestWorkerSIGKILLDuringAlignment:
    """Kill a worker mid-alignment, verify recovery."""

    def test_sigkill_during_processing(
        self, tmp_path: Path
    ) -> None:
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=4, num_r=1
        )
        work_dir = tmp_path / "work"

        # Spawn 2 slow workers (3s per package)
        procs = []
        for _ in range(2):
            p = multiprocessing.Process(
                target=_slow_worker_target,
                args=(
                    str(work_dir),
                    str(chunks_dir),
                    str(results_dir),
                    3.0,
                ),
            )
            p.start()
            procs.append(p)

        # Wait for packages to enter running
        time.sleep(1.0)
        assert stack.status()["RUNNING"] >= 1

        # Kill one worker
        os.kill(procs[0].pid, signal.SIGKILL)
        procs[0].join(timeout=2)

        # Make killed worker's packages stale
        for path in (work_dir / "running").glob("*.json"):
            data = json.loads(path.read_text())
            if procs[0].pid != os.getpid():
                # Approximate: make all running packages from ~1s ago stale
                old = datetime.now(tz=UTC) - timedelta(seconds=10)
                if data.get("heartbeat_at"):
                    hb = datetime.fromisoformat(data["heartbeat_at"])
                    if (datetime.now(tz=UTC) - hb).total_seconds() > 2:
                        data["heartbeat_at"] = old.isoformat()
                        path.write_text(json.dumps(data, indent=2))

        # Wait for surviving worker to finish everything
        _wait_for(stack, completed=4, timeout=30)

        # Clean up
        for p in procs:
            if p.is_alive():
                p.kill()
            p.join(timeout=2)

        assert stack.status()["COMPLETED"] == 4
        assert stack.status()["POISONED"] == 0


# === Single-process chaos tests (mock-based, fast) ===


class TestSimulatedOOM:
    """DIAMOND exit code 137 (OOM) — retry and eventually succeed or poison."""

    def test_oom_then_success(self, tmp_path: Path) -> None:
        """Fails twice with OOM, succeeds on 3rd attempt."""
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=1, num_r=1
        )

        call_count = 0

        def oom_then_success(
            query_fasta: object,
            ref_db: object,
            output_path: Path,
            **kwargs: object,
        ) -> DiamondResult:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return DiamondResult(
                    exit_code=137,
                    duration_seconds=1.0,
                    stderr="Killed",
                    error_message="DIAMOND killed by OOM (exit code 137).",
                )
            Path(str(output_path)).write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0"
                "\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0, duration_seconds=0.1, stderr="",
                output_path=str(output_path),
            )

        mock = MagicMock(spec=DiamondWrapper)
        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )
        mock.run_blastp.side_effect = oom_then_success

        runner = WorkerRunner(
            stack,
            mock,  # type: ignore[arg-type]
            chunks_dir,
            results_dir,
            max_idle_time=2.0,
        )
        completed = runner.run()

        assert completed == 1
        assert stack.status()["COMPLETED"] == 1

        # Check error history
        completed_dir = tmp_path / "work" / "completed"
        data = json.loads(
            list(completed_dir.glob("*.json"))[0].read_text()
        )
        assert len(data["error_history"]) == 2
        assert all("137" in e or "OOM" in e for e in data["error_history"])

    def test_oom_exhausts_retries(self, tmp_path: Path) -> None:
        """Always OOM → package poisoned."""
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=1, num_r=1
        )

        mock = MagicMock(spec=DiamondWrapper)
        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )
        mock.run_blastp.return_value = DiamondResult(
            exit_code=137,
            duration_seconds=1.0,
            stderr="Killed",
            error_message="DIAMOND killed by OOM (exit code 137).",
        )

        runner = WorkerRunner(
            stack,
            mock,  # type: ignore[arg-type]
            chunks_dir,
            results_dir,
            max_idle_time=2.0,
        )
        runner.run()

        assert stack.status()["POISONED"] == 1
        assert stack.status()["COMPLETED"] == 0


class TestIntermittentFailures:
    """DIAMOND fails every other call — all packages eventually succeed."""

    def test_alternating_failures(self, tmp_path: Path) -> None:
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=4, num_r=1
        )

        call_count = 0

        def alternating(
            query_fasta: object,
            ref_db: object,
            output_path: Path,
            **kwargs: object,
        ) -> DiamondResult:
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 1:
                return DiamondResult(
                    exit_code=1,
                    duration_seconds=0.01,
                    stderr="transient error",
                    error_message="Transient DIAMOND failure",
                )
            Path(str(output_path)).write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0"
                "\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0, duration_seconds=0.01, stderr="",
                output_path=str(output_path),
            )

        mock = MagicMock(spec=DiamondWrapper)
        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )
        mock.run_blastp.side_effect = alternating

        runner = WorkerRunner(
            stack,
            mock,  # type: ignore[arg-type]
            chunks_dir,
            results_dir,
            max_idle_time=2.0,
        )
        completed = runner.run()

        assert completed == 4
        assert stack.status()["COMPLETED"] == 4
        assert stack.status()["POISONED"] == 0

        # Some packages should have error history
        completed_dir = tmp_path / "work" / "completed"
        has_errors = sum(
            1
            for f in completed_dir.glob("*.json")
            if json.loads(f.read_text())["error_history"]
        )
        assert has_errors > 0


class TestCorruptWorkPackage:
    """Corrupt JSON in pending/ — worker skips it gracefully."""

    def test_corrupt_json_skipped(self, tmp_path: Path) -> None:
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=1, num_r=1
        )
        work_dir = tmp_path / "work"

        # Write a corrupt JSON file into pending/
        corrupt = work_dir / "pending" / "wp_corrupt.json"
        corrupt.write_text("{this is not valid json!!")

        diamond = _make_mock_diamond()
        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            max_idle_time=1.0,
        )
        completed = runner.run()

        # Valid package should still be processed
        assert completed == 1
        assert stack.status()["COMPLETED"] == 1

        # Corrupt file should be in poisoned/ (not stuck in pending/
        # or running/)
        assert not corrupt.exists()
        poisoned_files = list(
            (work_dir / "poisoned").glob("wp_corrupt*")
        )
        assert len(poisoned_files) == 1

    def test_wrong_schema_skipped(self, tmp_path: Path) -> None:
        """Valid JSON but wrong schema → treated as corrupt."""
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=1, num_r=1
        )
        work_dir = tmp_path / "work"

        # Valid JSON, wrong schema
        bad = work_dir / "pending" / "wp_bad_schema.json"
        bad.write_text(json.dumps({"foo": "bar"}))

        diamond = _make_mock_diamond()
        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            max_idle_time=1.0,
        )
        completed = runner.run()

        assert completed == 1
        assert not bad.exists()


class TestResultWriteFailure:
    """Simulated disk full — Parquet write fails."""

    def test_write_failure_fails_package(
        self, tmp_path: Path
    ) -> None:
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=2, num_r=1
        )

        call_count = 0

        def write_then_fail(
            query_fasta: object,
            ref_db: object,
            output_path: Path,
            **kwargs: object,
        ) -> DiamondResult:
            nonlocal call_count
            call_count += 1
            Path(str(output_path)).write_text(
                "seq_0001\tref_0001\t85.0\t100\t15\t0"
                "\t1\t100\t1\t100\t1e-30\t200.0\n"
            )
            return DiamondResult(
                exit_code=0, duration_seconds=0.01, stderr="",
                output_path=str(output_path),
            )

        mock = MagicMock(spec=DiamondWrapper)
        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.01, stderr=""
        )
        mock.run_blastp.side_effect = write_then_fail

        # Patch pq.write_table to fail on the first call only
        original_write = pq.write_table
        first_write = True

        def patched_write(table: object, path: object, **kw: object) -> None:
            nonlocal first_write
            if first_write:
                first_write = False
                msg = "No space left on device"
                raise OSError(msg)
            original_write(table, path, **kw)  # type: ignore[arg-type]

        with patch(
            "distributed_alignment.worker.runner.pq.write_table",
            side_effect=patched_write,
        ):
            runner = WorkerRunner(
                stack,
                mock,  # type: ignore[arg-type]
                chunks_dir,
                results_dir,
                max_idle_time=2.0,
            )
            runner.run()

        # One package should have failed (write error) and been retried
        # or eventually completed. The other should be fine.
        status = stack.status()
        assert status["COMPLETED"] >= 1
        # The worker loop continues after the write failure
        assert status["COMPLETED"] + status["POISONED"] == 2


@pytest.mark.integration
class TestAllWorkersDieThenRestart:
    """All workers die, new workers start and recover."""

    def test_full_cluster_restart(self, tmp_path: Path) -> None:
        stack, chunks_dir, results_dir = _setup_work(
            tmp_path, num_q=4, num_r=1
        )
        work_dir = tmp_path / "work"

        # Spawn 2 slow workers
        procs = []
        for _ in range(2):
            p = multiprocessing.Process(
                target=_slow_worker_target,
                args=(
                    str(work_dir),
                    str(chunks_dir),
                    str(results_dir),
                    5.0,
                ),
            )
            p.start()
            procs.append(p)

        # Let them claim packages
        time.sleep(1.0)
        assert stack.status()["RUNNING"] >= 1

        # Kill ALL workers
        for p in procs:
            os.kill(p.pid, signal.SIGKILL)
        for p in procs:
            p.join(timeout=2)

        # Make all running packages stale
        for path in (work_dir / "running").glob("*.json"):
            data = json.loads(path.read_text())
            old = datetime.now(tz=UTC) - timedelta(seconds=300)
            data["heartbeat_at"] = old.isoformat()
            path.write_text(json.dumps(data, indent=2))

        running_before = stack.status()["RUNNING"]
        assert running_before > 0

        # Spawn 2 NEW workers — should recover everything
        new_procs = []
        for _ in range(2):
            p = multiprocessing.Process(
                target=_slow_worker_target,
                args=(
                    str(work_dir),
                    str(chunks_dir),
                    str(results_dir),
                    0.1,  # Fast this time
                ),
            )
            p.start()
            new_procs.append(p)

        # Wait for completion
        _wait_for(stack, completed=4, timeout=30)

        for p in new_procs:
            if p.is_alive():
                p.kill()
            p.join(timeout=2)

        assert stack.status()["COMPLETED"] == 4
        assert stack.status()["POISONED"] == 0
