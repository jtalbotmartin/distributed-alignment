"""Result merger — DuckDB-based join and deduplication across reference chunks.

When all work packages for a given query chunk are complete, the merger
reads all per-ref-chunk result Parquet files, deduplicates hits, applies
global ranking, and writes a single merged Parquet file.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

if TYPE_CHECKING:
    from pathlib import Path

logger = structlog.get_logger(component="merger")

# Column name mapping: DIAMOND format 6 → MergedHit model
_DIAMOND_TO_MERGED: dict[str, str] = {
    "qseqid": "query_id",
    "sseqid": "subject_id",
    "pident": "percent_identity",
    "length": "alignment_length",
    "mismatch": "mismatches",
    "gapopen": "gap_opens",
    "qstart": "query_start",
    "qend": "query_end",
    "sstart": "subject_start",
    "send": "subject_end",
    "evalue": "evalue",
    "bitscore": "bitscore",
}

# Output schema matching MergedHit model (TDD section 3.4)
MERGED_SCHEMA = pa.schema(
    [
        pa.field("query_id", pa.string()),
        pa.field("subject_id", pa.string()),
        pa.field("percent_identity", pa.float64()),
        pa.field("alignment_length", pa.int32()),
        pa.field("mismatches", pa.int32()),
        pa.field("gap_opens", pa.int32()),
        pa.field("query_start", pa.int32()),
        pa.field("query_end", pa.int32()),
        pa.field("subject_start", pa.int32()),
        pa.field("subject_end", pa.int32()),
        pa.field("evalue", pa.float64()),
        pa.field("bitscore", pa.float64()),
        pa.field("global_rank", pa.int32()),
        pa.field("query_chunk_id", pa.string()),
        pa.field("ref_chunk_id", pa.string()),
    ]
)


def merge_query_chunk(
    query_chunk_id: str,
    results_dir: Path,
    output_dir: Path,
    *,
    top_n: int = 50,
    expected_ref_chunks: list[str],
) -> Path:
    """Merge alignment results across reference chunks for one query chunk.

    Reads all per-ref-chunk result Parquet files for the given query chunk,
    deduplicates hits (keeping best evalue per query-subject pair),
    applies global top-N ranking per query, and writes merged output.

    Args:
        query_chunk_id: ID of the query chunk to merge.
        results_dir: Directory containing per-work-package result files,
            named ``{query_chunk_id}_{ref_chunk_id}.parquet``.
        output_dir: Directory to write the merged output file.
        top_n: Maximum number of hits to keep per query sequence.
        expected_ref_chunks: List of reference chunk IDs that should
            have result files. Raises if any are missing.

    Returns:
        Path to the merged output Parquet file.

    Raises:
        ValueError: If any expected result files are missing.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check completeness — all expected ref chunks must have results
    result_paths: dict[str, Path] = {}
    missing: list[str] = []

    for ref_chunk_id in expected_ref_chunks:
        path = results_dir / f"{query_chunk_id}_{ref_chunk_id}.parquet"
        if path.exists():
            result_paths[ref_chunk_id] = path
        else:
            missing.append(ref_chunk_id)

    if missing:
        msg = (
            f"Incomplete merge for query chunk '{query_chunk_id}': "
            f"missing result files for ref chunks: {missing}"
        )
        raise ValueError(msg)

    log = logger.bind(
        query_chunk_id=query_chunk_id,
        num_result_files=len(result_paths),
    )
    log.info("merge_started")

    # Build the rename clause for DuckDB SQL
    rename_cols = ", ".join(
        f"{diamond_name} AS {merged_name}"
        for diamond_name, merged_name in _DIAMOND_TO_MERGED.items()
    )

    # Build DuckDB query over all result files
    con = duckdb.connect()

    try:
        # Register each result file with its ref_chunk_id
        union_parts: list[str] = []
        for ref_chunk_id, path in sorted(result_paths.items()):
            table_alias = f"t_{ref_chunk_id}"
            con.execute(
                f"CREATE VIEW {table_alias} AS "
                f"SELECT {rename_cols}, "
                f"'{query_chunk_id}' AS query_chunk_id, "
                f"'{ref_chunk_id}' AS ref_chunk_id "
                f"FROM read_parquet('{path}')"
            )
            union_parts.append(f"SELECT * FROM {table_alias}")

        union_sql = " UNION ALL ".join(union_parts)

        # Step 1: Deduplicate — keep best evalue per (query_id, subject_id)
        # Step 2: Global rank — top-N per query_id by evalue ASC, bitscore DESC
        merge_sql = f"""
            WITH all_hits AS (
                {union_sql}
            ),
            deduped AS (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY query_id, subject_id
                            ORDER BY evalue ASC, bitscore DESC
                        ) AS dedup_rank
                    FROM all_hits
                )
                WHERE dedup_rank = 1
            ),
            ranked AS (
                SELECT
                    query_id, subject_id, percent_identity,
                    alignment_length, mismatches, gap_opens,
                    query_start, query_end, subject_start, subject_end,
                    evalue, bitscore, query_chunk_id, ref_chunk_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY query_id
                        ORDER BY evalue ASC, bitscore DESC
                    ) AS global_rank
                FROM deduped
            )
            SELECT
                query_id, subject_id, percent_identity,
                alignment_length, mismatches, gap_opens,
                query_start, query_end, subject_start, subject_end,
                evalue, bitscore,
                CAST(global_rank AS INTEGER) AS global_rank,
                query_chunk_id, ref_chunk_id
            FROM ranked
            WHERE global_rank <= {top_n}
            ORDER BY query_id, global_rank
        """

        result = con.execute(merge_sql)
        arrow_table = result.arrow().read_all()

        # Count stats for logging
        total_before = con.execute(
            f"SELECT COUNT(*) FROM ({union_sql})"
        ).fetchone()
        total_before_count = total_before[0] if total_before else 0

        dedup_count = con.execute(
            f"""SELECT COUNT(*) FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY query_id, subject_id
                    ORDER BY evalue ASC, bitscore DESC
                ) AS rn FROM ({union_sql})
            ) WHERE rn = 1"""
        ).fetchone()
        dedup_total = dedup_count[0] if dedup_count else 0

    finally:
        con.close()

    # Cast to match the exact output schema
    merged_table = arrow_table.cast(MERGED_SCHEMA)

    output_path = output_dir / f"merged_{query_chunk_id}.parquet"
    pq.write_table(merged_table, output_path)

    log.info(
        "merge_completed",
        total_hits_raw=total_before_count,
        total_hits_deduped=dedup_total,
        total_hits_after_topn=merged_table.num_rows,
        output_path=str(output_path),
    )

    return output_path
