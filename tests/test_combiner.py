"""Tests for the feature combiner (Task 3.5)."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — used at runtime in fixtures

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.features.alignment_features import (
    extract_alignment_features,
)
from distributed_alignment.features.combiner import (
    COMBINED_SCHEMA,
    COMBINED_SCHEMA_NO_EMBEDDINGS,
    combine_features,
)
from distributed_alignment.features.embedding_features import (
    EMBEDDING_DIM,
    EMBEDDING_SCHEMA,
)
from distributed_alignment.features.kmer_features import (
    extract_kmer_features,
)
from distributed_alignment.ingest.chunker import CHUNK_SCHEMA
from distributed_alignment.taxonomy.enricher import ENRICHED_SCHEMA

# ---------------------------------------------------------------------------
# Helpers — build small synthetic fixtures inline
# ---------------------------------------------------------------------------


def _write_query_chunks(
    chunks_dir: Path,
    sequences: list[tuple[str, str]],
) -> None:
    """Write mock query chunk Parquet."""
    query_dir = chunks_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "chunk_id": pa.array(["q000"] * len(sequences), type=pa.string()),
            "sequence_id": pa.array([s[0] for s in sequences], type=pa.string()),
            "description": pa.array(
                [f">{s[0]}" for s in sequences],
                type=pa.string(),
            ),
            "sequence": pa.array([s[1] for s in sequences], type=pa.string()),
            "length": pa.array(
                [len(s[1]) for s in sequences],
                type=pa.int32(),
            ),
            "content_hash": pa.array(["abc123"] * len(sequences), type=pa.string()),
        },
        schema=CHUNK_SCHEMA,
    )
    pq.write_table(table, str(query_dir / "chunk_q000.parquet"))


def _enriched_hit(
    query_id: str = "Q001",
    subject_id: str = "sp|P0A8M3|SYT_ECOLI",
    percent_identity: float = 95.0,
    alignment_length: int = 100,
    evalue: float = 1e-50,
    global_rank: int = 1,
    phylum: str = "Pseudomonadota",
    kingdom: str = "Bacteria",
) -> dict[str, object]:
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
        "bitscore": 200.0,
        "global_rank": global_rank,
        "query_chunk_id": "q000",
        "ref_chunk_id": "r000",
        "taxon_id": 83333,
        "species": "Escherichia coli",
        "genus": "Escherichia",
        "family": "Enterobacteriaceae",
        "order": "Enterobacterales",
        "class": "Gammaproteobacteria",
        "phylum": phylum,
        "kingdom": kingdom,
    }


def _write_enriched(path: Path, rows: list[dict[str, object]]) -> Path:
    arrays: dict[str, list[object]] = {f.name: [] for f in ENRICHED_SCHEMA}
    for row in rows:
        for name in arrays:
            arrays[name].append(row.get(name))
    table = pa.table(
        {
            name: pa.array(vals, type=field.type)
            for (name, vals), field in zip(
                arrays.items(),
                ENRICHED_SCHEMA,
                strict=True,
            )
        },
    )
    pq.write_table(table, str(path))
    return path


def _write_fake_embeddings(
    path: Path,
    seq_ids: list[str],
) -> Path:
    """Write embeddings Parquet for given sequence IDs."""
    n = len(seq_ids)
    now = datetime.now(tz=UTC)
    flat = [0.1] * (n * EMBEDDING_DIM)
    table = pa.table(
        {
            "sequence_id": pa.array(seq_ids, type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(flat, type=pa.float32()),
                list_size=EMBEDDING_DIM,
            ),
            "feature_version": pa.array(["v1"] * n, type=pa.string()),
            "run_id": pa.array(["test"] * n, type=pa.string()),
            "created_at": pa.array(
                [now] * n,
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=EMBEDDING_SCHEMA,
    )
    pq.write_table(table, str(path))
    return path


# ---------------------------------------------------------------------------
# Fixture: 3 queries, Q001 with hits, Q002/Q003 zero-hit
# ---------------------------------------------------------------------------

_SEQUENCES = [
    ("Q001", "ACDEFGHIKLMNPQRSTVWY"),
    ("Q002", "MMMMMMMMMM"),
    ("Q003", "AAACCCGGG"),
]


@pytest.fixture()
def feature_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Produce alignment + kmer feature Parquets from small fixtures.

    Returns (alignment_path, kmer_path, chunks_dir).
    """
    chunks_dir = tmp_path / "work"
    _write_query_chunks(chunks_dir, _SEQUENCES)

    enriched = _write_enriched(
        tmp_path / "enriched.parquet",
        [
            _enriched_hit(query_id="Q001", global_rank=1),
            _enriched_hit(
                query_id="Q001",
                global_rank=2,
                phylum="Bacillota",
            ),
        ],
    )

    align_path = tmp_path / "alignment.parquet"
    kmer_path = tmp_path / "kmer.parquet"

    extract_alignment_features(enriched, chunks_dir, align_path, run_id="test")
    extract_kmer_features(chunks_dir, kmer_path, run_id="test")

    return align_path, kmer_path, chunks_dir


# ---------------------------------------------------------------------------
# Core joins
# ---------------------------------------------------------------------------


class TestCoreJoins:
    def test_combine_without_embeddings(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        table = pq.read_table(str(out))

        assert table.num_rows == 3
        assert "esm2_embedding" not in table.schema.names
        assert "kmer_frequencies" in table.schema.names

    def test_combine_with_embeddings(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        emb = _write_fake_embeddings(
            tmp_path / "emb.parquet",
            ["Q001", "Q002", "Q003"],
        )
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "r1",
            embeddings_path=emb,
        )
        table = pq.read_table(str(out))

        assert table.num_rows == 3
        assert "esm2_embedding" in table.schema.names
        vec = table.column("esm2_embedding")[0].as_py()
        assert len(vec) == EMBEDDING_DIM

    def test_row_count_matches_alignment(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        align_table = pq.read_table(str(align))
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        combined = pq.read_table(str(out))
        assert combined.num_rows == align_table.num_rows

    def test_zero_hit_queries_preserved(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        rows = pq.read_table(str(out)).to_pydict()

        ids = rows["sequence_id"]
        # Q002 and Q003 have zero hits
        for zero_hit in ("Q002", "Q003"):
            idx = ids.index(zero_hit)
            assert rows["hit_count"][idx] == 0
            assert rows["mean_percent_identity"][idx] is None
            # But they have k-mer vectors
            assert rows["kmer_frequencies"][idx] is not None
            assert len(rows["kmer_frequencies"][idx]) == 8000


# ---------------------------------------------------------------------------
# Schema and metadata
# ---------------------------------------------------------------------------


class TestSchemaAndMetadata:
    def test_schema_without_embeddings(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        table = pq.read_table(str(out))
        assert table.schema.equals(COMBINED_SCHEMA_NO_EMBEDDINGS)

    def test_schema_with_embeddings(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        emb = _write_fake_embeddings(
            tmp_path / "emb.parquet",
            ["Q001", "Q002", "Q003"],
        )
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "r1",
            embeddings_path=emb,
        )
        table = pq.read_table(str(out))
        assert table.schema.equals(COMBINED_SCHEMA)

    def test_feature_version_propagates(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "r1",
            feature_version="v2",
        )
        rows = pq.read_table(str(out)).to_pydict()
        assert all(v == "v2" for v in rows["feature_version"])

    def test_run_id_propagates(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "my-run-42",
        )
        rows = pq.read_table(str(out)).to_pydict()
        assert all(v == "my-run-42" for v in rows["run_id"])

    def test_created_at_utc(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        table = pq.read_table(str(out))
        ts_type = table.schema.field("created_at").type
        assert ts_type == pa.timestamp("us", tz="UTC")

    def test_input_metadata_dropped(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        """No duplicate metadata columns from input streams."""
        align, kmer, _ = feature_paths
        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        table = pq.read_table(str(out))
        # Only one of each metadata column
        for col in ("feature_version", "run_id", "created_at"):
            assert table.schema.names.count(col) == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_embeddings_missing_for_some(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        """Embeddings for 1 of 3 sequences → others get null."""
        align, kmer, _ = feature_paths
        emb = _write_fake_embeddings(tmp_path / "emb.parquet", ["Q001"])
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "r1",
            embeddings_path=emb,
        )
        rows = pq.read_table(str(out)).to_pydict()

        ids = rows["sequence_id"]
        q1_idx = ids.index("Q001")
        q2_idx = ids.index("Q002")

        assert rows["esm2_embedding"][q1_idx] is not None
        assert rows["esm2_embedding"][q2_idx] is None

    def test_extra_embeddings_dropped(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        """Embeddings for sequences not in query set are dropped."""
        align, kmer, _ = feature_paths
        emb = _write_fake_embeddings(
            tmp_path / "emb.parquet",
            ["Q001", "Q002", "Q003", "EXTRA1", "EXTRA2"],
        )
        out = combine_features(
            align,
            kmer,
            tmp_path / "combined.parquet",
            "r1",
            embeddings_path=emb,
        )
        table = pq.read_table(str(out))
        assert table.num_rows == 3
        ids = set(table.column("sequence_id").to_pylist())
        assert "EXTRA1" not in ids

    def test_mismatched_ids_raises(self, tmp_path: Path) -> None:
        """Alignment and k-mer with different IDs → ValueError."""
        # Write alignment features for Q001
        chunks1 = tmp_path / "c1"
        _write_query_chunks(chunks1, [("Q001", "ACDEFGHIKLMNPQRSTVWY")])
        enriched = _write_enriched(
            tmp_path / "enriched.parquet",
            [_enriched_hit(query_id="Q001")],
        )
        align_path = tmp_path / "align.parquet"
        extract_alignment_features(enriched, chunks1, align_path, run_id="test")

        # Write kmer features for Q002 (different!)
        chunks2 = tmp_path / "c2"
        _write_query_chunks(chunks2, [("Q002", "MMMMMMMMMM")])
        kmer_path = tmp_path / "kmer.parquet"
        extract_kmer_features(chunks2, kmer_path, run_id="test")

        with pytest.raises(ValueError, match="mismatch"):
            combine_features(
                align_path,
                kmer_path,
                tmp_path / "out.parquet",
                "r1",
            )

    def test_empty_inputs(self, tmp_path: Path) -> None:
        """Zero-row inputs → valid empty Parquet."""
        chunks = tmp_path / "work"
        _write_query_chunks(chunks, [])
        enriched = _write_enriched(tmp_path / "enriched.parquet", [])
        align = tmp_path / "align.parquet"
        kmer = tmp_path / "kmer.parquet"
        extract_alignment_features(enriched, chunks, align, run_id="test")
        extract_kmer_features(chunks, kmer, run_id="test")

        out = combine_features(align, kmer, tmp_path / "combined.parquet", "r1")
        table = pq.read_table(str(out))
        assert table.num_rows == 0

    def test_missing_embedding_file_raises(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        with pytest.raises(FileNotFoundError):
            combine_features(
                align,
                kmer,
                tmp_path / "combined.parquet",
                "r1",
                embeddings_path=Path("/nonexistent.parquet"),
            )


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_deterministic_output(
        self,
        feature_paths: tuple[Path, Path, Path],
        tmp_path: Path,
    ) -> None:
        align, kmer, _ = feature_paths
        out1 = combine_features(align, kmer, tmp_path / "c1.parquet", "r1")
        out2 = combine_features(align, kmer, tmp_path / "c2.parquet", "r1")
        t1 = pq.read_table(str(out1))
        t2 = pq.read_table(str(out2))

        # Compare all non-timestamp columns
        for col in t1.schema.names:
            if col == "created_at":
                continue
            assert t1.column(col).to_pylist() == t2.column(col).to_pylist(), (
                f"Mismatch in column: {col}"
            )
