"""Tests for the worker runner.

Unit tests mock DiamondWrapper to test the worker loop logic.
Integration tests (marked @pytest.mark.integration) use real DIAMOND.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

if TYPE_CHECKING:
    from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.ingest.chunker import CHUNK_SCHEMA, chunk_sequences
from distributed_alignment.ingest.fasta_parser import parse_fasta
from distributed_alignment.models import (
    ChunkEntry,
    ChunkManifest,
    ProteinSequence,
)
from distributed_alignment.scheduler.filesystem_backend import (
    FileSystemWorkStack,
)
from distributed_alignment.worker.diamond_wrapper import (
    DIAMOND_SCHEMA,
    DiamondResult,
    DiamondWrapper,
)
from distributed_alignment.worker.runner import (
    WorkerRunner,
    parquet_chunk_to_fasta,
)


def _make_test_sequences(n: int) -> list[ProteinSequence]:
    """Generate n test sequences."""
    amino = "ACDEFGHIKLMNPQRSTVWY"
    return [
        ProteinSequence(
            id=f"seq_{i:04d}",
            description=f"seq_{i:04d} test protein {i}",
            sequence=amino * 3,
            length=len(amino) * 3,
        )
        for i in range(n)
    ]


def _make_manifests(
    num_q: int, num_r: int
) -> tuple[ChunkManifest, ChunkManifest]:
    """Create minimal manifests for test work package generation."""
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


def _write_fake_result(output_path: Path) -> None:
    """Write a minimal DIAMOND-format TSV for mock results."""
    output_path.write_text(
        "query_0001\tref_0001\t85.0\t100\t15\t0\t1\t100\t1\t100\t1e-30\t200.0\n"
    )


class TestParquetChunkToFasta:
    """Tests for the Parquet → FASTA conversion helper."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parquet → FASTA → sequences match original."""
        sequences = _make_test_sequences(5)
        chunk_sequences(
            sequences,
            num_chunks=1,
            output_dir=tmp_path / "chunks",
            chunk_prefix="q",
            run_id="test",
            input_files=["test.fasta"],
        )

        parquet_file = list((tmp_path / "chunks").glob("chunk_*.parquet"))[0]
        fasta_file = tmp_path / "output.fasta"

        count = parquet_chunk_to_fasta(parquet_file, fasta_file)

        assert count == 5
        assert fasta_file.exists()

        # Parse the FASTA back and verify
        recovered = list(parse_fasta(fasta_file))
        original_ids = {s.id for s in sequences}
        recovered_ids = {s.id for s in recovered}
        assert recovered_ids == original_ids

    def test_empty_chunk(self, tmp_path: Path) -> None:
        """Empty Parquet → empty FASTA, count=0."""
        empty_table = pa.table(
            {
                "chunk_id": pa.array([], type=pa.string()),
                "sequence_id": pa.array([], type=pa.string()),
                "description": pa.array([], type=pa.string()),
                "sequence": pa.array([], type=pa.string()),
                "length": pa.array([], type=pa.int32()),
                "content_hash": pa.array([], type=pa.string()),
            },
            schema=CHUNK_SCHEMA,
        )
        parquet_path = tmp_path / "empty.parquet"
        pq.write_table(empty_table, parquet_path)

        fasta_path = tmp_path / "empty.fasta"
        count = parquet_chunk_to_fasta(parquet_path, fasta_path)

        assert count == 0


class TestWorkerRunnerUnit:
    """Unit tests for the worker runner using a mocked DiamondWrapper."""

    def _setup_chunks_and_stack(
        self, tmp_path: Path
    ) -> tuple[FileSystemWorkStack, Path, Path]:
        """Create chunk files and work packages for testing."""
        chunks_dir = tmp_path / "chunks"
        results_dir = tmp_path / "results"

        # Create real chunk parquet files
        sequences = _make_test_sequences(20)
        chunk_sequences(
            sequences[:10],
            num_chunks=1,
            output_dir=chunks_dir / "queries",
            chunk_prefix="q",
            run_id="test",
            input_files=["q.fasta"],
        )
        chunk_sequences(
            sequences[10:],
            num_chunks=1,
            output_dir=chunks_dir / "refs",
            chunk_prefix="r",
            run_id="test",
            input_files=["r.fasta"],
        )

        # Set up work stack with packages
        stack = FileSystemWorkStack(tmp_path / "work")
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        return stack, chunks_dir, results_dir

    def _make_mock_diamond(
        self, tmp_path: Path, *, fail: bool = False
    ) -> DiamondWrapper:
        """Create a mock DiamondWrapper that writes fake output."""
        mock = MagicMock(spec=DiamondWrapper)

        mock.make_db.return_value = DiamondResult(
            exit_code=0, duration_seconds=0.1, stderr=""
        )

        if fail:
            mock.run_blastp.return_value = DiamondResult(
                exit_code=1,
                duration_seconds=0.1,
                stderr="fake error",
                error_message="Fake DIAMOND failure",
            )
        else:

            def fake_blastp(
                query_fasta: Path,
                ref_db: Path,
                output_path: Path,
                **kwargs: object,
            ) -> DiamondResult:
                _write_fake_result(output_path)
                return DiamondResult(
                    exit_code=0,
                    duration_seconds=0.5,
                    stderr="",
                    output_path=str(output_path),
                )

            mock.run_blastp.side_effect = fake_blastp

        return mock  # type: ignore[return-value]

    def test_processes_all_packages(self, tmp_path: Path) -> None:
        stack, chunks_dir, results_dir = self._setup_chunks_and_stack(
            tmp_path
        )
        diamond = self._make_mock_diamond(tmp_path)

        runner = WorkerRunner(
            stack, diamond, chunks_dir, results_dir
        )
        completed = runner.run()

        assert completed == 1
        assert stack.pending_count() == 0
        assert stack.status()["COMPLETED"] == 1

    def test_writes_result_parquet(self, tmp_path: Path) -> None:
        stack, chunks_dir, results_dir = self._setup_chunks_and_stack(
            tmp_path
        )
        diamond = self._make_mock_diamond(tmp_path)

        runner = WorkerRunner(
            stack, diamond, chunks_dir, results_dir
        )
        runner.run()

        result_files = list(results_dir.glob("*.parquet"))
        assert len(result_files) == 1

        table = pq.read_table(result_files[0])
        assert table.schema.equals(DIAMOND_SCHEMA)

    def test_diamond_failure_exhausts_retries(
        self, tmp_path: Path
    ) -> None:
        """Persistent failure → worker retries until POISONED."""
        stack, chunks_dir, results_dir = self._setup_chunks_and_stack(
            tmp_path
        )
        diamond = self._make_mock_diamond(tmp_path, fail=True)

        runner = WorkerRunner(
            stack, diamond, chunks_dir, results_dir
        )
        completed = runner.run()

        assert completed == 0
        assert stack.status()["COMPLETED"] == 0
        # Worker keeps retrying until max_attempts exhausted → POISONED
        assert stack.status()["POISONED"] == 1
        assert stack.pending_count() == 0

    def test_makedb_failure_exhausts_retries(
        self, tmp_path: Path
    ) -> None:
        """Persistent makedb failure → retries exhausted → POISONED."""
        stack, chunks_dir, results_dir = self._setup_chunks_and_stack(
            tmp_path
        )
        diamond = self._make_mock_diamond(tmp_path)
        diamond.make_db.return_value = DiamondResult(  # type: ignore[attr-defined]
            exit_code=1,
            duration_seconds=0.1,
            stderr="makedb error",
            error_message="makedb failed",
        )

        runner = WorkerRunner(
            stack, diamond, chunks_dir, results_dir
        )
        completed = runner.run()

        assert completed == 0
        assert stack.status()["POISONED"] == 1
        assert stack.pending_count() == 0

    def test_missing_chunk_calls_fail(self, tmp_path: Path) -> None:
        """When chunk files don't exist, fail gracefully."""
        stack = FileSystemWorkStack(tmp_path / "work")
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        empty_chunks = tmp_path / "empty_chunks"
        empty_chunks.mkdir()
        results_dir = tmp_path / "results"
        diamond = self._make_mock_diamond(tmp_path)

        runner = WorkerRunner(
            stack, diamond, empty_chunks, results_dir
        )
        completed = runner.run()

        assert completed == 0

    def test_worker_has_unique_id(self, tmp_path: Path) -> None:
        stack = FileSystemWorkStack(tmp_path)
        diamond = MagicMock(spec=DiamondWrapper)

        runner1 = WorkerRunner(
            stack, diamond, tmp_path, tmp_path / "results1"
        )
        runner2 = WorkerRunner(
            stack, diamond, tmp_path, tmp_path / "results2"
        )

        assert runner1.worker_id != runner2.worker_id
        assert runner1.worker_id.startswith("worker-")

    def test_multiple_packages(self, tmp_path: Path) -> None:
        """Worker processes multiple packages sequentially."""
        chunks_dir = tmp_path / "chunks"
        results_dir = tmp_path / "results"

        sequences = _make_test_sequences(30)
        chunk_sequences(
            sequences[:10],
            num_chunks=1,
            output_dir=chunks_dir / "queries",
            chunk_prefix="q",
            run_id="test",
            input_files=["q.fasta"],
        )
        chunk_sequences(
            sequences[10:20],
            num_chunks=1,
            output_dir=chunks_dir / "refs1",
            chunk_prefix="r",
            run_id="test",
            input_files=["r1.fasta"],
        )

        # 1 query × 1 ref = 1 package — but let's make 2 ref chunks
        # by creating another ref chunk file manually
        chunk_sequences(
            sequences[20:30],
            num_chunks=1,
            output_dir=chunks_dir / "refs2",
            chunk_prefix="s",
            run_id="test",
            input_files=["r2.fasta"],
        )

        stack = FileSystemWorkStack(tmp_path / "work")
        q_manifest, r_manifest = _make_manifests(1, 1)
        stack.generate_work_packages(q_manifest, r_manifest)

        diamond = self._make_mock_diamond(tmp_path)
        runner = WorkerRunner(
            stack, diamond, chunks_dir, results_dir
        )
        completed = runner.run()

        assert completed == 1
        assert stack.pending_count() == 0


# --- Integration tests (require DIAMOND binary) ---


@pytest.mark.integration
class TestWorkerRunnerIntegration:
    """Full integration test with real DIAMOND.

    Uses Swiss-Prot fixture data when available, synthetic fallback otherwise.
    """

    def test_full_pipeline(
        self,
        integration_test_data: tuple[Path, Path],
        tmp_path: Path,
    ) -> None:
        """End-to-end: ingest → chunk → schedule → worker → result."""
        diamond = DiamondWrapper(binary="diamond", threads=1)
        if not diamond.check_available():
            pytest.skip("DIAMOND binary not available")

        queries_fasta, ref_fasta = integration_test_data

        # Ingest and chunk
        chunks_dir = tmp_path / "chunks"
        q_seqs = list(parse_fasta(queries_fasta))
        r_seqs = list(parse_fasta(ref_fasta))

        q_manifest = chunk_sequences(
            q_seqs,
            num_chunks=1,
            output_dir=chunks_dir / "queries",
            chunk_prefix="q",
            run_id="integration_test",
            input_files=["queries.fasta"],
        )
        r_manifest = chunk_sequences(
            r_seqs,
            num_chunks=1,
            output_dir=chunks_dir / "refs",
            chunk_prefix="r",
            run_id="integration_test",
            input_files=["reference.fasta"],
        )

        # Schedule
        stack = FileSystemWorkStack(tmp_path / "work")
        stack.generate_work_packages(q_manifest, r_manifest)

        assert stack.pending_count() == 1

        # Run worker
        results_dir = tmp_path / "results"
        runner = WorkerRunner(
            stack,
            diamond,
            chunks_dir,
            results_dir,
            sensitivity="fast",
            max_target_seqs=5,
            timeout=120,
        )
        completed = runner.run()

        assert completed == 1
        assert stack.status()["COMPLETED"] == 1
        assert stack.pending_count() == 0

        # Verify result
        result_files = list(results_dir.glob("*.parquet"))
        assert len(result_files) == 1

        table = pq.read_table(result_files[0])
        assert table.schema.equals(DIAMOND_SCHEMA)
