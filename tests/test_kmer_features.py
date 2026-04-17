"""Tests for Stream B: k-mer frequency feature extraction."""

from __future__ import annotations

from pathlib import Path  # noqa: TCH003 — used at runtime in fixtures

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.features.kmer_features import (
    KMER_SCHEMA,
    KMER_VOCABULARY,
    _kmer_frequencies,
    extract_kmer_features,
)
from distributed_alignment.ingest.chunker import CHUNK_SCHEMA

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_query_chunks(
    chunks_dir: Path,
    sequences: list[tuple[str, str]],
    *,
    num_chunks: int = 1,
) -> None:
    """Write mock query chunk Parquet files.

    *sequences* is a list of ``(sequence_id, sequence)`` pairs.
    When *num_chunks* > 1, sequences are round-robin split across
    multiple chunk files.
    """
    query_dir = chunks_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)

    buckets: list[list[tuple[str, str]]] = [[] for _ in range(num_chunks)]
    for i, seq in enumerate(sequences):
        buckets[i % num_chunks].append(seq)

    for idx, bucket in enumerate(buckets):
        if not bucket:
            continue
        table = pa.table(
            {
                "chunk_id": pa.array(
                    [f"q{idx:03d}"] * len(bucket),
                    type=pa.string(),
                ),
                "sequence_id": pa.array([s[0] for s in bucket], type=pa.string()),
                "description": pa.array(
                    [f">{s[0]}" for s in bucket],
                    type=pa.string(),
                ),
                "sequence": pa.array([s[1] for s in bucket], type=pa.string()),
                "length": pa.array(
                    [len(s[1]) for s in bucket],
                    type=pa.int32(),
                ),
                "content_hash": pa.array(["abc123"] * len(bucket), type=pa.string()),
            },
            schema=CHUNK_SCHEMA,
        )
        pq.write_table(
            table,
            str(query_dir / f"chunk_q{idx:03d}.parquet"),
        )


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------


class TestVocabulary:
    def test_vocabulary_length(self) -> None:
        assert len(KMER_VOCABULARY) == 8000

    def test_first_kmer(self) -> None:
        assert KMER_VOCABULARY[0] == "AAA"

    def test_last_kmer(self) -> None:
        assert KMER_VOCABULARY[-1] == "YYY"

    def test_all_standard_amino_acids(self) -> None:
        chars = {c for kmer in KMER_VOCABULARY for c in kmer}
        assert chars == set("ACDEFGHIKLMNPQRSTVWY")


# ---------------------------------------------------------------------------
# Hand-verified counts
# ---------------------------------------------------------------------------


class TestHandVerifiedCounts:
    def test_aaaa(self) -> None:
        """'AAAA' → 2 x AAA → freq 1.0 at AAA index."""
        freqs = _kmer_frequencies("AAAA")
        idx = KMER_VOCABULARY.index("AAA")
        assert freqs[idx] == pytest.approx(1.0)
        # All other positions zero
        assert sum(freqs) == pytest.approx(1.0)

    def test_acacacac(self) -> None:
        """'ACACACAC' → ACA,CAC,ACA,CAC,ACA,CAC → 0.5/0.5."""
        freqs = _kmer_frequencies("ACACACAC")
        aca = KMER_VOCABULARY.index("ACA")
        cac = KMER_VOCABULARY.index("CAC")
        assert freqs[aca] == pytest.approx(0.5)
        assert freqs[cac] == pytest.approx(0.5)
        assert sum(freqs) == pytest.approx(1.0)

    def test_aaaccc(self) -> None:
        """'AAACCC' → AAA,AAC,ACC,CCC → 0.25 each."""
        freqs = _kmer_frequencies("AAACCC")
        for kmer in ("AAA", "AAC", "ACC", "CCC"):
            idx = KMER_VOCABULARY.index(kmer)
            assert freqs[idx] == pytest.approx(0.25)
        assert sum(freqs) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Summation invariant
# ---------------------------------------------------------------------------


class TestSummationInvariant:
    @pytest.mark.parametrize(
        "seq",
        [
            "ACDEFGHIKLMNPQRSTVWY",
            "MMMMMM",
            "ACACACACACACACAC",
            "MKWVTFISLLFLFSSAYS",
        ],
    )
    def test_frequencies_sum_to_one(self, seq: str) -> None:
        freqs = _kmer_frequencies(seq)
        assert sum(freqs) == pytest.approx(1.0, abs=1e-5)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_too_short(self) -> None:
        """Sequence shorter than k=3 → all-zero vector."""
        freqs = _kmer_frequencies("AA")
        assert all(v == 0.0 for v in freqs)
        assert len(freqs) == 8000

    def test_empty_string(self) -> None:
        freqs = _kmer_frequencies("")
        assert all(v == 0.0 for v in freqs)

    def test_nonstandard_residues_skipped(self) -> None:
        """'AAAXAAA' → skip AAX, AXA, XAA; keep AAA(0), AAA(4)."""
        freqs = _kmer_frequencies("AAAXAAA")
        idx = KMER_VOCABULARY.index("AAA")
        assert freqs[idx] == pytest.approx(1.0)
        # Only 2 valid 3-mers, both AAA
        assert sum(freqs) == pytest.approx(1.0)

    def test_all_nonstandard(self) -> None:
        """All non-standard → zero vector."""
        freqs = _kmer_frequencies("XXXBBB")
        assert all(v == 0.0 for v in freqs)

    def test_lowercase_handled(self) -> None:
        """Lowercase input is uppercased before counting."""
        freqs_lower = _kmer_frequencies("aaaa")
        freqs_upper = _kmer_frequencies("AAAA")
        assert freqs_lower == freqs_upper


# ---------------------------------------------------------------------------
# Schema and metadata
# ---------------------------------------------------------------------------


class TestSchemaAndMetadata:
    def test_output_matches_schema(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", "ACDEFGHIKLMNPQRSTVWY")])
        out = extract_kmer_features(tmp_path, tmp_path / "kmers.parquet", "test-run")
        table = pq.read_table(str(out))
        assert table.schema.equals(KMER_SCHEMA)

    def test_metadata_values(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", "ACDEFGHIKLMNPQRSTVWY")])
        out = extract_kmer_features(
            tmp_path,
            tmp_path / "kmers.parquet",
            "my-run",
            feature_version="v1",
        )
        row = pq.read_table(str(out)).to_pydict()
        assert row["feature_version"][0] == "v1"
        assert row["run_id"][0] == "my-run"
        assert row["created_at"][0] is not None


# ---------------------------------------------------------------------------
# Integration with chunk files
# ---------------------------------------------------------------------------


class TestChunkIntegration:
    def test_multi_chunk_all_sequences(self, tmp_path: Path) -> None:
        """N sequences across multiple chunks → N output rows."""
        seqs = [
            ("Q001", "ACDEFGHIKLMNPQRSTVWY"),
            ("Q002", "MMMMMM"),
            ("Q003", "AAACCC"),
            ("Q004", "ACACACAC"),
            ("Q005", "MKWVTFISLLFLFSSAYS"),
        ]
        _write_query_chunks(tmp_path, seqs, num_chunks=3)
        out = extract_kmer_features(tmp_path, tmp_path / "kmers.parquet", "run")
        table = pq.read_table(str(out))

        assert table.num_rows == 5
        ids = set(table.column("sequence_id").to_pylist())
        assert ids == {"Q001", "Q002", "Q003", "Q004", "Q005"}

    def test_frequency_vector_length(self, tmp_path: Path) -> None:
        """Each kmer_frequencies entry has exactly 8000 elements."""
        _write_query_chunks(tmp_path, [("Q001", "ACDEFGHIKLMNPQRSTVWY")])
        out = extract_kmer_features(tmp_path, tmp_path / "kmers.parquet", "run")
        table = pq.read_table(str(out))
        vec = table.column("kmer_frequencies")[0].as_py()
        assert len(vec) == 8000


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_identical_output(self, tmp_path: Path) -> None:
        """Same input → identical kmer_frequencies columns."""
        _write_query_chunks(
            tmp_path,
            [
                ("Q001", "ACDEFGHIKLMNPQRSTVWY"),
                ("Q002", "MMMMMM"),
            ],
        )
        out1 = extract_kmer_features(tmp_path, tmp_path / "k1.parquet", "run")
        out2 = extract_kmer_features(tmp_path, tmp_path / "k2.parquet", "run")
        t1 = pq.read_table(str(out1))
        t2 = pq.read_table(str(out2))

        assert (
            t1.column("sequence_id").to_pylist() == t2.column("sequence_id").to_pylist()
        )
        assert (
            t1.column("kmer_frequencies").to_pylist()
            == t2.column("kmer_frequencies").to_pylist()
        )
