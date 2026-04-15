"""Tests for the DuckDB-based result merger.

All tests create synthetic Parquet fixtures directly — no DIAMOND required.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_alignment.merge.merger import MERGED_SCHEMA, merge_query_chunk
from distributed_alignment.worker.diamond_wrapper import DIAMOND_SCHEMA

if TYPE_CHECKING:
    from pathlib import Path


def _write_result_parquet(
    path: Path,
    rows: list[dict[str, str | float | int]],
) -> None:
    """Write a synthetic result Parquet file with DIAMOND schema.

    Args:
        path: Output path.
        rows: List of dicts with DIAMOND column names.
    """
    if not rows:
        # Empty table with correct schema
        table = pa.table(
            {
                name: pa.array([], type=dtype)
                for name, dtype in [
                    ("qseqid", pa.string()),
                    ("sseqid", pa.string()),
                    ("pident", pa.float64()),
                    ("length", pa.int32()),
                    ("mismatch", pa.int32()),
                    ("gapopen", pa.int32()),
                    ("qstart", pa.int32()),
                    ("qend", pa.int32()),
                    ("sstart", pa.int32()),
                    ("send", pa.int32()),
                    ("evalue", pa.float64()),
                    ("bitscore", pa.float64()),
                ]
            },
            schema=DIAMOND_SCHEMA,
        )
    else:
        table = pa.table(
            {col: [r[col] for r in rows] for col in rows[0]},
            schema=DIAMOND_SCHEMA,
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _make_hit(
    qseqid: str = "query_001",
    sseqid: str = "ref_001",
    pident: float = 85.0,
    length: int = 100,
    mismatch: int = 15,
    gapopen: int = 0,
    qstart: int = 1,
    qend: int = 100,
    sstart: int = 1,
    send: int = 100,
    evalue: float = 1e-30,
    bitscore: float = 200.0,
) -> dict[str, str | float | int]:
    """Create a single DIAMOND hit row with sensible defaults."""
    return {
        "qseqid": qseqid,
        "sseqid": sseqid,
        "pident": pident,
        "length": length,
        "mismatch": mismatch,
        "gapopen": gapopen,
        "qstart": qstart,
        "qend": qend,
        "sstart": sstart,
        "send": send,
        "evalue": evalue,
        "bitscore": bitscore,
    }


class TestMergeGlobalRanking:
    """Tests for correct global ranking by evalue."""

    def test_merge_three_ref_chunks(self, tmp_path: Path) -> None:
        """Merge results from 3 ref chunks → global rank by evalue."""
        results_dir = tmp_path / "results"

        # Ref chunk r000: one hit with evalue 1e-20
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit(sseqid="ref_A", evalue=1e-20, bitscore=150.0)],
        )
        # Ref chunk r001: one hit with evalue 1e-40 (best)
        _write_result_parquet(
            results_dir / "q000_r001.parquet",
            [_make_hit(sseqid="ref_B", evalue=1e-40, bitscore=300.0)],
        )
        # Ref chunk r002: one hit with evalue 1e-10
        _write_result_parquet(
            results_dir / "q000_r002.parquet",
            [_make_hit(sseqid="ref_C", evalue=1e-10, bitscore=80.0)],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000", "r001", "r002"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 3

        # Global rank 1 should be the best evalue (1e-40, ref_B)
        ranks = table.column("global_rank").to_pylist()
        subjects = table.column("subject_id").to_pylist()
        evalues = table.column("evalue").to_pylist()

        assert ranks == [1, 2, 3]
        assert subjects[0] == "ref_B"
        assert evalues[0] == pytest.approx(1e-40)
        assert subjects[2] == "ref_C"
        assert evalues[2] == pytest.approx(1e-10)

    def test_tiebreak_by_bitscore(self, tmp_path: Path) -> None:
        """Same evalue → higher bitscore ranks first."""
        results_dir = tmp_path / "results"

        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [
                _make_hit(sseqid="ref_A", evalue=1e-30, bitscore=200.0),
                _make_hit(sseqid="ref_B", evalue=1e-30, bitscore=250.0),
            ],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        subjects = table.column("subject_id").to_pylist()

        # ref_B has higher bitscore → rank 1
        assert subjects[0] == "ref_B"
        assert subjects[1] == "ref_A"


class TestDeduplication:
    """Tests for same (query, subject) pair across ref chunks."""

    def test_dedup_keeps_best_evalue(self, tmp_path: Path) -> None:
        """Same (query, subject) in two ref chunks → keep best evalue."""
        results_dir = tmp_path / "results"

        # Same subject in both ref chunks, different evalues
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit(sseqid="ref_shared", evalue=1e-20, bitscore=150.0)],
        )
        _write_result_parquet(
            results_dir / "q000_r001.parquet",
            [_make_hit(sseqid="ref_shared", evalue=1e-40, bitscore=300.0)],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000", "r001"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 1
        assert table.column("evalue")[0].as_py() == pytest.approx(1e-40)

    def test_dedup_different_queries_not_affected(self, tmp_path: Path) -> None:
        """Different queries hitting the same subject are NOT deduped."""
        results_dir = tmp_path / "results"

        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [
                _make_hit(qseqid="query_A", sseqid="ref_shared", evalue=1e-20),
                _make_hit(qseqid="query_B", sseqid="ref_shared", evalue=1e-30),
            ],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        # Both queries should be present
        assert table.num_rows == 2


class TestTopN:
    """Tests for top-N filtering per query."""

    def test_top_n_limits_hits(self, tmp_path: Path) -> None:
        """8 hits for one query with top_n=5 → 5 hits in output."""
        results_dir = tmp_path / "results"

        hits = [
            _make_hit(sseqid=f"ref_{i:03d}", evalue=10.0 ** (-i * 5)) for i in range(8)
        ]
        _write_result_parquet(results_dir / "q000_r000.parquet", hits)

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            top_n=5,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 5

        ranks = table.column("global_rank").to_pylist()
        assert ranks == [1, 2, 3, 4, 5]

    def test_top_n_per_query_not_global(self, tmp_path: Path) -> None:
        """Two queries each with 4 hits, top_n=3 → 3+3=6 total."""
        results_dir = tmp_path / "results"

        hits = [
            _make_hit(
                qseqid="query_A",
                sseqid=f"ref_A{i}",
                evalue=10.0 ** (-i * 5),
            )
            for i in range(4)
        ] + [
            _make_hit(
                qseqid="query_B",
                sseqid=f"ref_B{i}",
                evalue=10.0 ** (-i * 5),
            )
            for i in range(4)
        ]
        _write_result_parquet(results_dir / "q000_r000.parquet", hits)

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            top_n=3,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 6

        # Check each query has exactly 3 hits
        query_ids = table.column("query_id").to_pylist()
        assert query_ids.count("query_A") == 3
        assert query_ids.count("query_B") == 3


class TestSchema:
    """Tests for output schema validation."""

    def test_output_schema_matches_merged_hit(self, tmp_path: Path) -> None:
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.schema.equals(MERGED_SCHEMA)

    def test_column_names(self, tmp_path: Path) -> None:
        """Verify DIAMOND columns are renamed to MergedHit names."""
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        expected_cols = [
            "query_id",
            "subject_id",
            "percent_identity",
            "alignment_length",
            "mismatches",
            "gap_opens",
            "query_start",
            "query_end",
            "subject_start",
            "subject_end",
            "evalue",
            "bitscore",
            "global_rank",
            "query_chunk_id",
            "ref_chunk_id",
        ]
        assert table.column_names == expected_cols

    def test_chunk_ids_in_output(self, tmp_path: Path) -> None:
        """query_chunk_id and ref_chunk_id are set correctly."""
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )
        _write_result_parquet(
            results_dir / "q000_r001.parquet",
            [_make_hit(sseqid="ref_other", evalue=1e-50)],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000", "r001"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        q_chunks = set(table.column("query_chunk_id").to_pylist())
        r_chunks = set(table.column("ref_chunk_id").to_pylist())
        assert q_chunks == {"q000"}
        assert r_chunks == {"r000", "r001"}


class TestIncompleteMerge:
    """Tests for missing result files."""

    def test_missing_ref_chunk_raises(self, tmp_path: Path) -> None:
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )
        # r001 and r002 are missing

        with pytest.raises(ValueError, match="missing result files"):
            merge_query_chunk(
                "q000",
                results_dir,
                tmp_path / "merged",
                expected_ref_chunks=["r000", "r001", "r002"],
            )

    def test_error_lists_missing_chunks(self, tmp_path: Path) -> None:
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )

        with pytest.raises(ValueError, match="r001.*r002|r002.*r001"):
            merge_query_chunk(
                "q000",
                results_dir,
                tmp_path / "merged",
                expected_ref_chunks=["r000", "r001", "r002"],
            )


class TestEmptyResults:
    """Tests for empty result files."""

    def test_all_empty_results_produce_empty_output(self, tmp_path: Path) -> None:
        """All result files exist but have zero rows → valid empty Parquet."""
        results_dir = tmp_path / "results"
        _write_result_parquet(results_dir / "q000_r000.parquet", [])
        _write_result_parquet(results_dir / "q000_r001.parquet", [])

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000", "r001"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 0
        assert table.schema.equals(MERGED_SCHEMA)

    def test_mix_of_empty_and_nonempty(self, tmp_path: Path) -> None:
        """One empty ref chunk, one with hits → only hits appear."""
        results_dir = tmp_path / "results"
        _write_result_parquet(results_dir / "q000_r000.parquet", [])
        _write_result_parquet(
            results_dir / "q000_r001.parquet",
            [_make_hit(sseqid="ref_only")],
        )

        output_dir = tmp_path / "merged"
        merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000", "r001"],
        )

        table = pq.read_table(output_dir / "merged_q000.parquet")
        assert table.num_rows == 1
        assert table.column("subject_id")[0].as_py() == "ref_only"


class TestReturnValue:
    """Tests for the return value."""

    def test_returns_output_path(self, tmp_path: Path) -> None:
        results_dir = tmp_path / "results"
        _write_result_parquet(
            results_dir / "q000_r000.parquet",
            [_make_hit()],
        )

        output_dir = tmp_path / "merged"
        result = merge_query_chunk(
            "q000",
            results_dir,
            output_dir,
            expected_ref_chunks=["r000"],
        )

        assert result == output_dir / "merged_q000.parquet"
        assert result.exists()
