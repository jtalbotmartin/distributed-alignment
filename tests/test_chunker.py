"""Tests for the deterministic hash-based sequence chunker."""

from __future__ import annotations

import hashlib
import json
import random
from pathlib import Path

import pyarrow.parquet as pq

from distributed_alignment.ingest.chunker import (
    CHUNK_SCHEMA,
    assign_chunk,
    chunk_sequences,
    file_checksum,
    sequence_content_hash,
)
from distributed_alignment.models import ChunkManifest, ProteinSequence


def _make_sequences(n: int) -> list[ProteinSequence]:
    """Generate n simple test ProteinSequence objects."""
    amino_acids = "ACDEFGHIKLMNPQRSTVWY"
    return [
        ProteinSequence(
            id=f"seq_{i:04d}",
            description=f"seq_{i:04d} test sequence {i}",
            sequence=amino_acids * 2,
            length=len(amino_acids) * 2,
        )
        for i in range(n)
    ]


class TestAssignChunk:
    """Tests for the chunk assignment hash function."""

    def test_deterministic(self) -> None:
        """Same ID always maps to the same chunk."""
        for _ in range(10):
            assert assign_chunk("seq_0001", 5) == assign_chunk("seq_0001", 5)

    def test_uses_sha256_not_builtin_hash(self) -> None:
        """Verify the function produces the expected SHA-256-based result."""
        seq_id = "seq_0001"
        num_chunks = 10
        expected = int(hashlib.sha256(seq_id.encode()).hexdigest(), 16) % num_chunks
        assert assign_chunk(seq_id, num_chunks) == expected

    def test_distributes_across_chunks(self) -> None:
        """100 sequence IDs across 5 chunks should use all 5."""
        assignments = {assign_chunk(f"seq_{i:04d}", 5) for i in range(100)}
        assert assignments == {0, 1, 2, 3, 4}


class TestSequenceContentHash:
    """Tests for the sequence content hash function."""

    def test_deterministic(self) -> None:
        assert sequence_content_hash("MKWVTFIS") == sequence_content_hash("MKWVTFIS")

    def test_different_sequences_different_hashes(self) -> None:
        assert sequence_content_hash("MKWVTFIS") != sequence_content_hash("ACDEFGHI")


class TestChunkDeterminism:
    """Tests that chunking is reproducible."""

    def test_identical_input_produces_identical_parquet(self, tmp_path: Path) -> None:
        """Chunk the same sequences twice → identical Parquet files."""
        sequences = _make_sequences(50)

        dir_a = tmp_path / "run_a"
        dir_b = tmp_path / "run_b"

        chunk_sequences(
            sequences,
            num_chunks=5,
            output_dir=dir_a,
            chunk_prefix="q",
            run_id="run_a",
            input_files=["test.fasta"],
        )
        chunk_sequences(
            sequences,
            num_chunks=5,
            output_dir=dir_b,
            chunk_prefix="q",
            run_id="run_b",
            input_files=["test.fasta"],
        )

        # Compare all parquet files by content checksum
        for parquet_a in sorted(dir_a.glob("chunk_*.parquet")):
            parquet_b = dir_b / parquet_a.name
            assert parquet_b.exists(), f"Missing {parquet_b.name} in run_b"
            assert file_checksum(parquet_a) == file_checksum(parquet_b)

    def test_shuffled_input_same_chunk_assignments(self, tmp_path: Path) -> None:
        """Shuffling input order → same sequences end up in same chunks."""
        sequences = _make_sequences(50)
        shuffled = list(sequences)
        random.Random(42).shuffle(shuffled)

        dir_ordered = tmp_path / "ordered"
        dir_shuffled = tmp_path / "shuffled"

        chunk_sequences(
            sequences,
            num_chunks=5,
            output_dir=dir_ordered,
            chunk_prefix="q",
            run_id="run_1",
            input_files=["test.fasta"],
        )
        chunk_sequences(
            shuffled,
            num_chunks=5,
            output_dir=dir_shuffled,
            chunk_prefix="q",
            run_id="run_2",
            input_files=["test.fasta"],
        )

        # Same parquet files should exist
        ordered_files = sorted(f.name for f in dir_ordered.glob("chunk_*.parquet"))
        shuffled_files = sorted(f.name for f in dir_shuffled.glob("chunk_*.parquet"))
        assert ordered_files == shuffled_files

        # Same content in each
        for name in ordered_files:
            assert file_checksum(dir_ordered / name) == file_checksum(
                dir_shuffled / name
            )


class TestRoundTrip:
    """Tests for zero-loss, zero-duplication round-trip."""

    def test_chunk_and_reassemble(self, tmp_path: Path) -> None:
        """Chunk → read all Parquet → reassemble → compare to original."""
        sequences = _make_sequences(100)
        original_ids = {s.id for s in sequences}
        original_seqs = {s.id: s.sequence for s in sequences}

        chunk_sequences(
            sequences,
            num_chunks=7,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="roundtrip",
            input_files=["test.fasta"],
        )

        # Read all chunks back
        recovered_ids: set[str] = set()
        recovered_seqs: dict[str, str] = {}

        for parquet_path in tmp_path.glob("chunk_*.parquet"):
            table = pq.read_table(parquet_path)
            for row_idx in range(table.num_rows):
                seq_id = table.column("sequence_id")[row_idx].as_py()
                seq = table.column("sequence")[row_idx].as_py()
                assert seq_id not in recovered_ids, f"Duplicate: {seq_id}"
                recovered_ids.add(seq_id)
                recovered_seqs[seq_id] = seq

        # Zero loss
        assert recovered_ids == original_ids
        # Zero corruption
        for seq_id, seq in recovered_seqs.items():
            assert seq == original_seqs[seq_id]


class TestChunkDistribution:
    """Tests for how sequences are distributed across chunks."""

    def test_approximately_uniform(self, tmp_path: Path) -> None:
        """100 sequences into 5 chunks → each has ~20 (within variance)."""
        sequences = _make_sequences(100)

        manifest = chunk_sequences(
            sequences,
            num_chunks=5,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="dist_test",
            input_files=["test.fasta"],
        )

        counts = [entry.num_sequences for entry in manifest.chunks]
        assert sum(counts) == 100
        # Each chunk should have between 5 and 40 (very generous bounds
        # for a hash-based distribution of 100 items into 5 buckets)
        for count in counts:
            assert 5 <= count <= 40, f"Chunk size {count} is outside expected range"

    def test_fewer_sequences_than_chunks(self, tmp_path: Path) -> None:
        """3 sequences into 10 chunks → only 1-3 Parquet files produced."""
        sequences = _make_sequences(3)

        manifest = chunk_sequences(
            sequences,
            num_chunks=10,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="sparse_test",
            input_files=["test.fasta"],
        )

        assert manifest.total_sequences == 3
        assert manifest.num_chunks == 10
        # Only non-empty chunks produce entries
        assert len(manifest.chunks) <= 3
        assert sum(e.num_sequences for e in manifest.chunks) == 3

        # Only as many parquet files as non-empty chunks
        parquet_files = list(tmp_path.glob("chunk_*.parquet"))
        assert len(parquet_files) == len(manifest.chunks)


class TestManifest:
    """Tests for manifest accuracy."""

    def test_manifest_sequence_counts(self, tmp_path: Path) -> None:
        sequences = _make_sequences(50)

        manifest = chunk_sequences(
            sequences,
            num_chunks=5,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="manifest_test",
            input_files=["test.fasta"],
        )

        assert manifest.total_sequences == 50
        assert sum(e.num_sequences for e in manifest.chunks) == 50
        assert manifest.chunking_strategy == "deterministic_hash"
        assert manifest.input_files == ["test.fasta"]

    def test_manifest_checksums_match_files(self, tmp_path: Path) -> None:
        sequences = _make_sequences(30)

        manifest = chunk_sequences(
            sequences,
            num_chunks=3,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="checksum_test",
            input_files=["test.fasta"],
        )

        for entry in manifest.chunks:
            actual = file_checksum(Path(entry.parquet_path))
            assert entry.content_checksum == actual

    def test_manifest_written_as_json(self, tmp_path: Path) -> None:
        sequences = _make_sequences(10)

        manifest = chunk_sequences(
            sequences,
            num_chunks=2,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="json_test",
            input_files=["test.fasta"],
        )

        # Verify the returned manifest is serialisable and round-trips
        data = json.loads(json.dumps(manifest.model_dump(mode="json")))
        loaded = ChunkManifest(**data)
        assert loaded.run_id == "json_test"
        assert loaded.total_sequences == 10

    def test_manifest_parquet_paths_exist(self, tmp_path: Path) -> None:
        sequences = _make_sequences(20)

        manifest = chunk_sequences(
            sequences,
            num_chunks=4,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="path_test",
            input_files=["test.fasta"],
        )

        for entry in manifest.chunks:
            assert Path(entry.parquet_path).exists()


class TestParquetSchema:
    """Tests for the output Parquet schema."""

    def test_schema_matches_tdd_spec(self, tmp_path: Path) -> None:
        sequences = _make_sequences(10)

        chunk_sequences(
            sequences,
            num_chunks=2,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="schema_test",
            input_files=["test.fasta"],
        )

        for parquet_path in tmp_path.glob("chunk_*.parquet"):
            table = pq.read_table(parquet_path)
            assert table.schema.equals(CHUNK_SCHEMA)

    def test_content_hash_is_sha256(self, tmp_path: Path) -> None:
        seq = ProteinSequence(
            id="test_seq",
            description="test_seq for hash check",
            sequence="MKWVTFIS",
            length=8,
        )

        chunk_sequences(
            [seq],
            num_chunks=1,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="hash_test",
            input_files=["test.fasta"],
        )

        table = pq.read_table(list(tmp_path.glob("chunk_*.parquet"))[0])
        stored_hash = table.column("content_hash")[0].as_py()
        expected_hash = hashlib.sha256(b"MKWVTFIS").hexdigest()
        assert stored_hash == expected_hash


class TestEdgeCases:
    """Edge case tests."""

    def test_zero_sequences(self, tmp_path: Path) -> None:
        manifest = chunk_sequences(
            [],
            num_chunks=5,
            output_dir=tmp_path,
            chunk_prefix="q",
            run_id="empty_test",
            input_files=["empty.fasta"],
        )

        assert manifest.total_sequences == 0
        assert manifest.chunks == []
        assert list(tmp_path.glob("chunk_*.parquet")) == []

    def test_invalid_num_chunks_raises(self, tmp_path: Path) -> None:
        import pytest

        with pytest.raises(ValueError, match="num_chunks must be positive"):
            chunk_sequences(
                [],
                num_chunks=0,
                output_dir=tmp_path,
                chunk_prefix="q",
                run_id="bad_test",
                input_files=["test.fasta"],
            )

    def test_output_dir_created_if_missing(self, tmp_path: Path) -> None:
        output = tmp_path / "nested" / "dir"
        assert not output.exists()

        chunk_sequences(
            _make_sequences(5),
            num_chunks=2,
            output_dir=output,
            chunk_prefix="q",
            run_id="mkdir_test",
            input_files=["test.fasta"],
        )

        assert output.exists()
        assert len(list(output.glob("chunk_*.parquet"))) > 0
