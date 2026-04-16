"""Taxonomic enrichment of merged alignment results.

Adds taxonomy columns (species through kingdom) to merged Parquet
output by looking up each subject accession in a :class:`TaxonomyDB`,
and computes per-query taxonomic profiles for downstream feature
engineering.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TCH003 — used at runtime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from distributed_alignment.taxonomy.ncbi_loader import TaxonomyDB  # noqa: TCH001

logger = structlog.get_logger(__name__)

# Taxonomy column names in output order.
_TAXONOMY_COLUMNS: list[str] = [
    "species",
    "genus",
    "family",
    "order",
    "class",
    "phylum",
    "kingdom",
]

ENRICHED_SCHEMA = pa.schema(
    [
        # ── Original merged columns ──
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
        # ── Taxonomy columns ──
        pa.field("taxon_id", pa.int32()),
        pa.field("species", pa.string()),
        pa.field("genus", pa.string()),
        pa.field("family", pa.string()),
        pa.field("order", pa.string()),
        pa.field("class", pa.string()),
        pa.field("phylum", pa.string()),
        pa.field("kingdom", pa.string()),
    ]
)

PROFILE_SCHEMA = pa.schema(
    [
        pa.field("query_id", pa.string()),
        pa.field("phylum", pa.string()),
        pa.field("hit_count", pa.int64()),
        pa.field("mean_percent_identity", pa.float64()),
        pa.field("best_evalue", pa.float64()),
    ]
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def extract_accession(subject_id: str) -> str:
    """Extract UniProt accession from a subject identifier.

    Handles Swiss-Prot/TrEMBL pipe-delimited format and plain accessions::

        "sp|P0A8M3|SYT_ECOLI"  → "P0A8M3"
        "tr|A0A0F7ABC|NAME"    → "A0A0F7ABC"
        "P0A8M3"               → "P0A8M3"
    """
    if "|" in subject_id:
        parts = subject_id.split("|")
        if len(parts) >= 2:  # noqa: PLR2004
            return parts[1]
    return subject_id


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------


def enrich_results(
    merged_parquet_path: Path,
    taxonomy_db: TaxonomyDB,
    output_path: Path,
) -> Path:
    """Add taxonomy columns to merged alignment results.

    For every hit, the subject accession is extracted from ``subject_id``,
    looked up in *taxonomy_db*, and the resulting lineage fields are
    appended as new columns.  Accessions that cannot be mapped receive
    ``"unknown"`` for all taxonomy string columns and a null ``taxon_id``.

    Args:
        merged_parquet_path: Path to merged Parquet (MERGED_SCHEMA).
        taxonomy_db: A loaded :class:`TaxonomyDB` instance.
        output_path: Where to write the enriched Parquet.

    Returns:
        *output_path* for convenience.
    """
    merged = pq.read_table(str(merged_parquet_path))

    # Fast-path: empty input → empty output with enriched schema.
    if len(merged) == 0:
        empty = pa.table(
            {f.name: pa.array([], type=f.type) for f in ENRICHED_SCHEMA},
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(empty, str(output_path))
        logger.info("enrich_empty_input", output=str(output_path))
        return output_path

    subject_ids: list[str] = merged.column("subject_id").to_pylist()

    # ── Deduplicate lookups ───────────────────────────────────────────
    unique_sids = set(subject_ids)
    acc_for_sid = {sid: extract_accession(sid) for sid in unique_sids}
    unique_accessions = set(acc_for_sid.values())

    # Batch lookup: accession → lineage + taxon_id
    lineage_cache: dict[str, dict[str, str | None]] = {}
    taxid_cache: dict[str, int | None] = {}
    for acc in unique_accessions:
        lineage_cache[acc] = taxonomy_db.get_lineage_for_accession(acc)
        taxid_cache[acc] = taxonomy_db.get_taxon_id_for_accession(acc)

    unmapped = sum(
        1
        for acc in unique_accessions
        if all(v is None for v in lineage_cache[acc].values())
    )
    if unmapped:
        logger.warning(
            "unmapped_accessions",
            count=unmapped,
            total=len(unique_accessions),
        )

    # ── Build column arrays ───────────────────────────────────────────
    taxon_ids: list[int | None] = []
    col_arrays: dict[str, list[str]] = {col: [] for col in _TAXONOMY_COLUMNS}

    for sid in subject_ids:
        acc = acc_for_sid[sid]
        lineage = lineage_cache[acc]
        taxon_ids.append(taxid_cache[acc])
        for col in _TAXONOMY_COLUMNS:
            col_arrays[col].append(lineage.get(col) or "unknown")

    # Append taxonomy columns to merged table
    enriched = merged.append_column("taxon_id", pa.array(taxon_ids, type=pa.int32()))
    for col in _TAXONOMY_COLUMNS:
        enriched = enriched.append_column(
            col, pa.array(col_arrays[col], type=pa.string())
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(enriched, str(output_path))

    logger.info(
        "enrichment_complete",
        rows=len(enriched),
        unmapped=unmapped,
        output=str(output_path),
    )
    return output_path


# ---------------------------------------------------------------------------
# Taxonomic profiles
# ---------------------------------------------------------------------------


def compute_taxonomic_profiles(
    enriched_parquet_path: Path,
    output_path: Path,
) -> Path:
    """Compute per-query taxonomic hit distributions.

    For each ``(query_id, phylum)`` pair, aggregates hit count, mean
    percent identity, and best (lowest) e-value.  Output feeds into
    taxonomic entropy computation in feature engineering.

    Args:
        enriched_parquet_path: Enriched Parquet from :func:`enrich_results`.
        output_path: Where to write the profile Parquet.

    Returns:
        *output_path* for convenience.
    """
    conn = duckdb.connect()
    try:
        result = (
            conn.execute(
                """
            SELECT
                query_id,
                phylum,
                CAST(COUNT(*) AS BIGINT) AS hit_count,
                AVG(percent_identity) AS mean_percent_identity,
                MIN(evalue) AS best_evalue
            FROM read_parquet($1)
            GROUP BY query_id, phylum
            ORDER BY query_id, hit_count DESC
            """,
                [str(enriched_parquet_path)],
            )
            .arrow()
            .read_all()
        )
    finally:
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(result, str(output_path))

    logger.info(
        "taxonomic_profiles_complete",
        rows=len(result),
        output=str(output_path),
    )
    return output_path
