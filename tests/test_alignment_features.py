"""Tests for Stream A: alignment feature extraction."""

from __future__ import annotations

import math
from pathlib import Path  # noqa: TCH003 — used at runtime in fixtures

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.features.alignment_features import (
    FEATURE_SCHEMA,
    extract_alignment_features,
)
from distributed_alignment.ingest.chunker import CHUNK_SCHEMA
from distributed_alignment.taxonomy.enricher import ENRICHED_SCHEMA

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_enriched(path: Path, rows: list[dict[str, object]]) -> Path:
    """Write an enriched-format Parquet file from row dicts."""
    arrays: dict[str, list[object]] = {f.name: [] for f in ENRICHED_SCHEMA}
    for row in rows:
        for name in arrays:
            arrays[name].append(row.get(name))
    table = pa.table(
        {
            name: pa.array(vals, type=field.type)
            for (name, vals), field in zip(arrays.items(), ENRICHED_SCHEMA, strict=True)
        },
    )
    pq.write_table(table, str(path))
    return path


def _write_query_chunks(
    chunks_dir: Path,
    sequences: list[tuple[str, int]],
) -> None:
    """Write a mock query chunk Parquet with given (sequence_id, length) pairs."""
    query_dir = chunks_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "chunk_id": pa.array(["q000"] * len(sequences), type=pa.string()),
            "sequence_id": pa.array([s[0] for s in sequences], type=pa.string()),
            "description": pa.array([f">{s[0]}" for s in sequences], type=pa.string()),
            "sequence": pa.array(["M" * s[1] for s in sequences], type=pa.string()),
            "length": pa.array([s[1] for s in sequences], type=pa.int32()),
            "content_hash": pa.array(["abc123"] * len(sequences), type=pa.string()),
        },
        schema=CHUNK_SCHEMA,
    )
    pq.write_table(table, str(query_dir / "chunk_q000.parquet"))


def _hit(
    query_id: str = "Q001",
    subject_id: str = "sp|P0A8M3|SYT_ECOLI",
    percent_identity: float = 95.0,
    alignment_length: int = 100,
    evalue: float = 1e-50,
    bitscore: float = 200.0,
    global_rank: int = 1,
    phylum: str = "Pseudomonadota",
    kingdom: str = "Bacteria",
    taxon_id: int = 83333,
) -> dict[str, object]:
    """Build a single enriched-hit row with sensible defaults."""
    return {
        "query_id": query_id,
        "subject_id": subject_id,
        "percent_identity": percent_identity,
        "alignment_length": alignment_length,
        "mismatches": 5,
        "gap_opens": 0,
        "query_start": 1,
        "query_end": alignment_length,
        "subject_start": 1,
        "subject_end": alignment_length,
        "evalue": evalue,
        "bitscore": bitscore,
        "global_rank": global_rank,
        "query_chunk_id": "q000",
        "ref_chunk_id": "r000",
        "taxon_id": taxon_id,
        "species": "Escherichia coli",
        "genus": "Escherichia",
        "family": "Enterobacteriaceae",
        "order": "Enterobacterales",
        "class": "Gammaproteobacteria",
        "phylum": phylum,
        "kingdom": kingdom,
    }


# ---------------------------------------------------------------------------
# Basic features
# ---------------------------------------------------------------------------


class TestBasicFeatures:
    def test_hit_count_and_identity(self, tmp_path: Path) -> None:
        """Verify hit_count, mean/max identity for a query with 3 hits."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(percent_identity=90.0, global_rank=1),
                _hit(percent_identity=80.0, global_rank=2),
                _hit(percent_identity=70.0, global_rank=3),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        table = pq.read_table(str(out))
        row = table.to_pydict()

        assert row["hit_count"][0] == 3
        assert row["mean_percent_identity"][0] == pytest.approx(80.0)
        assert row["max_percent_identity"][0] == pytest.approx(90.0)

    def test_mean_alignment_length(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(alignment_length=100, global_rank=1),
                _hit(alignment_length=200, global_rank=2),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["mean_alignment_length"][0] == pytest.approx(150.0)

    def test_mean_evalue_log10(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(evalue=1e-50, global_rank=1),
                _hit(evalue=1e-30, global_rank=2),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        # AVG(log10(1e-50), log10(1e-30)) = AVG(-50, -30) = -40
        assert row["mean_evalue_log10"][0] == pytest.approx(-40.0)


# ---------------------------------------------------------------------------
# Taxonomic entropy
# ---------------------------------------------------------------------------


class TestTaxonomicEntropy:
    def test_single_phylum_entropy_zero(self, tmp_path: Path) -> None:
        """All hits in one phylum → entropy = 0."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(global_rank=1),
                _hit(global_rank=2),
                _hit(global_rank=3),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["taxonomic_entropy"][0] == pytest.approx(0.0)

    def test_uniform_three_phyla(self, tmp_path: Path) -> None:
        """One hit per phylum (3 phyla) → entropy = log2(3) ≈ 1.585."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(phylum="Pseudomonadota", kingdom="Bacteria", global_rank=1),
                _hit(phylum="Bacillota", kingdom="Bacteria", global_rank=2),
                _hit(phylum="Ascomycota", kingdom="Eukaryota", global_rank=3),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["taxonomic_entropy"][0] == pytest.approx(math.log2(3), rel=1e-4)

    def test_skewed_distribution(self, tmp_path: Path) -> None:
        """4 hits: 3 in A, 1 in B → entropy ≈ 0.811."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(phylum="Pseudomonadota", global_rank=1),
                _hit(phylum="Pseudomonadota", global_rank=2),
                _hit(phylum="Pseudomonadota", global_rank=3),
                _hit(phylum="Bacillota", global_rank=4),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        expected = -(0.75 * math.log2(0.75) + 0.25 * math.log2(0.25))
        assert row["taxonomic_entropy"][0] == pytest.approx(expected, rel=1e-4)


# ---------------------------------------------------------------------------
# Zero-hit queries
# ---------------------------------------------------------------------------


class TestZeroHitQueries:
    def test_zero_hit_query_included(self, tmp_path: Path) -> None:
        """Query with no alignment hits is included with hit_count=0."""
        _write_query_chunks(tmp_path, [("Q001", 200), ("Q002", 150)])
        # Only Q001 has hits
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit(query_id="Q001")],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        table = pq.read_table(str(out))
        rows = table.to_pydict()

        # Both queries present
        assert table.num_rows == 2
        ids = set(rows["sequence_id"])
        assert ids == {"Q001", "Q002"}

        # Find the zero-hit query
        idx = rows["sequence_id"].index("Q002")
        assert rows["hit_count"][idx] == 0
        assert rows["num_phyla"][idx] == 0
        assert rows["num_kingdoms"][idx] == 0
        # Float features should be null for zero-hit queries
        assert rows["mean_percent_identity"][idx] is None
        assert rows["taxonomic_entropy"][idx] is None
        assert rows["best_hit_query_coverage"][idx] is None

    def test_all_queries_no_hits(self, tmp_path: Path) -> None:
        """All queries have zero hits → all features are defaults."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(tmp_path / "enriched.parquet", [])
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        table = pq.read_table(str(out))

        assert table.num_rows == 1
        row = table.to_pydict()
        assert row["hit_count"][0] == 0
        assert row["num_phyla"][0] == 0


# ---------------------------------------------------------------------------
# Coverage and edge cases
# ---------------------------------------------------------------------------


class TestCoverageAndEdgeCases:
    def test_best_hit_query_coverage(self, tmp_path: Path) -> None:
        """Query of length 300, best hit alignment_length 240 → 0.8."""
        _write_query_chunks(tmp_path, [("Q001", 300)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit(alignment_length=240, global_rank=1)],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["best_hit_query_coverage"][0] == pytest.approx(0.8)

    def test_evalue_zero_no_inf(self, tmp_path: Path) -> None:
        """evalue=0.0 should not produce -inf in mean_evalue_log10."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit(evalue=0.0, global_rank=1)],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        val = row["mean_evalue_log10"][0]
        assert val is not None
        assert not math.isinf(val)
        assert val == pytest.approx(-999.0)

    def test_single_hit_std_is_null(self, tmp_path: Path) -> None:
        """STDDEV_SAMP of a single value is NULL (undefined)."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit(global_rank=1)],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["std_alignment_length"][0] is None

    def test_num_phyla_excludes_unknown(self, tmp_path: Path) -> None:
        """Phylum 'unknown' should not be counted."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(phylum="Pseudomonadota", kingdom="Bacteria", global_rank=1),
                _hit(phylum="unknown", kingdom="unknown", global_rank=2),
            ],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["num_phyla"][0] == 1
        assert row["num_kingdoms"][0] == 1


# ---------------------------------------------------------------------------
# Determinism and schema
# ---------------------------------------------------------------------------


class TestDeterminismAndSchema:
    def test_deterministic_output(self, tmp_path: Path) -> None:
        """Same input produces same output."""
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [
                _hit(global_rank=1),
                _hit(phylum="Bacillota", global_rank=2),
            ],
        )

        out1 = extract_alignment_features(enriched, tmp_path, tmp_path / "f1.parquet")
        out2 = extract_alignment_features(enriched, tmp_path, tmp_path / "f2.parquet")

        t1 = pq.read_table(str(out1))
        t2 = pq.read_table(str(out2))

        # Compare computed columns (skip created_at which has time-of-run)
        for col in [
            "sequence_id",
            "hit_count",
            "mean_percent_identity",
            "max_percent_identity",
            "taxonomic_entropy",
            "num_phyla",
        ]:
            assert t1.column(col).to_pylist() == t2.column(col).to_pylist()

    def test_output_schema_has_all_feature_columns(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit()],
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        table = pq.read_table(str(out))

        for field in FEATURE_SCHEMA:
            assert field.name in table.schema.names, (
                f"Missing feature column: {field.name}"
            )

    def test_all_queries_present(self, tmp_path: Path) -> None:
        """Output has one row per query sequence."""
        seqs = [("Q001", 200), ("Q002", 300), ("Q003", 150)]
        _write_query_chunks(tmp_path, seqs)
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit(query_id="Q001")],  # Only Q001 has hits
        )
        out = extract_alignment_features(
            enriched, tmp_path, tmp_path / "features.parquet"
        )
        table = pq.read_table(str(out))

        assert table.num_rows == 3
        assert set(table.column("sequence_id").to_pylist()) == {
            "Q001",
            "Q002",
            "Q003",
        }

    def test_metadata_columns_present(self, tmp_path: Path) -> None:
        _write_query_chunks(tmp_path, [("Q001", 200)])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_hit()],
        )
        out = extract_alignment_features(
            enriched,
            tmp_path,
            tmp_path / "features.parquet",
            run_id="test-run",
            feature_version="2.0",
        )
        row = pq.read_table(str(out)).to_pydict()

        assert row["run_id"][0] == "test-run"
        assert row["feature_version"][0] == "2.0"
        assert row["created_at"][0] is not None
