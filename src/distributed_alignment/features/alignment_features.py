"""Stream A: alignment-derived feature extraction.

Computes one feature row per query sequence from enriched alignment
results. Uses DuckDB for aggregation and includes queries with
zero hits (LEFT JOIN against the full query list from chunk files).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — used at runtime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger(__name__)

FEATURE_SCHEMA = pa.schema(
    [
        pa.field("sequence_id", pa.string()),
        pa.field("hit_count", pa.int64()),
        pa.field("mean_percent_identity", pa.float64()),
        pa.field("max_percent_identity", pa.float64()),
        pa.field("mean_evalue_log10", pa.float64()),
        pa.field("mean_alignment_length", pa.float64()),
        pa.field("std_alignment_length", pa.float64()),
        pa.field("best_hit_query_coverage", pa.float64()),
        pa.field("taxonomic_entropy", pa.float64()),
        pa.field("num_phyla", pa.int64()),
        pa.field("num_kingdoms", pa.int64()),
        pa.field("feature_version", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("created_at", pa.timestamp("us")),
    ]
)


def _read_parquet_path(path: Path) -> pa.Table:
    """Read Parquet from a file or a directory of files."""
    if path.is_file():
        return pq.read_table(str(path))
    if path.is_dir():
        files = sorted(path.glob("*.parquet"))
        if not files:
            msg = f"No Parquet files found in {path}"
            raise FileNotFoundError(msg)
        return pa.concat_tables([pq.read_table(str(f)) for f in files])
    msg = f"Path does not exist: {path}"
    raise FileNotFoundError(msg)


def _read_query_sequences(chunks_dir: Path) -> pa.Table:
    """Read all query sequence_id/length pairs from chunk Parquet files."""
    query_dir = chunks_dir / "queries"
    if not query_dir.exists():
        msg = f"Query chunks directory not found: {query_dir}"
        raise FileNotFoundError(msg)

    chunk_files = sorted(query_dir.glob("*.parquet"))
    if not chunk_files:
        return pa.table(
            {
                "sequence_id": pa.array([], type=pa.string()),
                "query_length": pa.array([], type=pa.int32()),
            }
        )

    tables = [
        pq.read_table(str(f), columns=["sequence_id", "length"]) for f in chunk_files
    ]
    combined = pa.concat_tables(tables)
    # Rename 'length' to 'query_length' to avoid collisions in SQL
    return combined.rename_columns(["sequence_id", "query_length"])


def extract_alignment_features(
    enriched_parquet_path: Path,
    chunks_dir: Path,
    output_path: Path,
    *,
    run_id: str = "",
    feature_version: str = "v1",
) -> Path:
    """Compute per-query alignment features from enriched results.

    Produces one row per query sequence (including zero-hit queries)
    with alignment statistics, taxonomic diversity metrics, and
    coverage.

    Args:
        enriched_parquet_path: Enriched Parquet file or directory
            (output of :func:`~..taxonomy.enricher.enrich_results`).
        chunks_dir: Pipeline work directory containing
            ``chunks/queries/*.parquet`` with sequence_id and length.
        output_path: Where to write the feature Parquet.
        run_id: Pipeline run identifier (metadata column).
        feature_version: Feature schema version (metadata column).

    Returns:
        *output_path* for convenience.
    """
    conn = duckdb.connect()
    try:
        # -- Load data -------------------------------------------------------
        enriched = _read_parquet_path(enriched_parquet_path)
        all_queries = _read_query_sequences(chunks_dir)

        conn.register("enriched", enriched)
        conn.register("all_queries", all_queries)

        # -- Compute features via SQL ------------------------------------
        result = (
            conn.execute(
                """
            WITH agg AS (
                SELECT
                    query_id,
                    COUNT(*) AS hit_count,
                    AVG(percent_identity)
                        AS mean_percent_identity,
                    MAX(percent_identity)
                        AS max_percent_identity,
                    AVG(CASE WHEN evalue > 0
                             THEN LOG10(evalue)
                             ELSE -999.0 END)
                        AS mean_evalue_log10,
                    AVG(CAST(alignment_length AS DOUBLE))
                        AS mean_alignment_length,
                    STDDEV_SAMP(
                        CAST(alignment_length AS DOUBLE))
                        AS std_alignment_length,
                    COUNT(DISTINCT CASE
                        WHEN phylum != 'unknown'
                        THEN phylum END)
                        AS num_phyla,
                    COUNT(DISTINCT CASE
                        WHEN kingdom != 'unknown'
                        THEN kingdom END)
                        AS num_kingdoms
                FROM enriched
                GROUP BY query_id
            ),

            best AS (
                SELECT
                    query_id,
                    alignment_length AS best_aln_len
                FROM enriched
                WHERE global_rank = 1
            ),

            phylum_dist AS (
                SELECT query_id, phylum,
                       COUNT(*) AS cnt
                FROM enriched
                WHERE phylum != 'unknown'
                GROUP BY query_id, phylum
            ),
            query_totals AS (
                SELECT query_id, SUM(cnt) AS total
                FROM phylum_dist
                GROUP BY query_id
            ),
            entropy AS (
                SELECT
                    pd.query_id,
                    -SUM(
                        (CAST(pd.cnt AS DOUBLE) / qt.total)
                        * LOG2(CAST(pd.cnt AS DOUBLE)
                               / qt.total)
                    ) AS taxonomic_entropy
                FROM phylum_dist pd
                JOIN query_totals qt
                  ON pd.query_id = qt.query_id
                GROUP BY pd.query_id
            )

            SELECT
                aq.sequence_id,
                COALESCE(a.hit_count, 0) AS hit_count,
                a.mean_percent_identity,
                a.max_percent_identity,
                a.mean_evalue_log10,
                a.mean_alignment_length,
                a.std_alignment_length,
                CASE
                    WHEN b.best_aln_len IS NOT NULL
                         AND aq.query_length > 0
                    THEN CAST(b.best_aln_len AS DOUBLE)
                       / CAST(aq.query_length AS DOUBLE)
                END AS best_hit_query_coverage,
                e.taxonomic_entropy,
                COALESCE(a.num_phyla, 0)
                    AS num_phyla,
                COALESCE(a.num_kingdoms, 0)
                    AS num_kingdoms
            FROM all_queries aq
            LEFT JOIN agg a
              ON aq.sequence_id = a.query_id
            LEFT JOIN best b
              ON aq.sequence_id = b.query_id
            LEFT JOIN entropy e
              ON aq.sequence_id = e.query_id
            ORDER BY aq.sequence_id
            """
            )
            .arrow()
            .read_all()
        )
    finally:
        conn.close()

    # -- Add metadata columns ------------------------------------------------
    n = result.num_rows
    now = datetime.now(tz=UTC)
    result = result.append_column(
        "feature_version", pa.array([feature_version] * n, type=pa.string())
    )
    result = result.append_column("run_id", pa.array([run_id] * n, type=pa.string()))
    result = result.append_column(
        "created_at",
        pa.array([now] * n, type=pa.timestamp("us", tz="UTC")),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(result, str(output_path))

    # -- Log summary ---------------------------------------------------------
    hit_counts = result.column("hit_count").to_pylist()
    with_hits = sum(1 for c in hit_counts if c > 0)
    without_hits = n - with_hits

    logger.info(
        "alignment_features_extracted",
        total_queries=n,
        with_hits=with_hits,
        without_hits=without_hits,
        mean_hit_count=sum(hit_counts) / n if n else 0,
        output=str(output_path),
    )

    return output_path
