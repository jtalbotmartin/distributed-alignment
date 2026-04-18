"""Feature combiner: join alignment, k-mer, and optional ESM-2 features.

Produces a single versioned feature table keyed by ``sequence_id``.
When embeddings are not provided, the ``esm2_embedding`` column is
absent from the output schema.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — used at runtime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from distributed_alignment.features.embedding_features import (
    load_embeddings,
)

logger = structlog.get_logger(__name__)

# -- Alignment feature columns (order matches FEATURE_SCHEMA minus metadata) --

_ALIGNMENT_COLS: list[str] = [
    "sequence_id",
    "hit_count",
    "mean_percent_identity",
    "max_percent_identity",
    "mean_evalue_log10",
    "mean_alignment_length",
    "std_alignment_length",
    "best_hit_query_coverage",
    "taxonomic_entropy",
    "num_phyla",
    "num_kingdoms",
]

# -- Combined schemas ---------------------------------------------------------

_COMBINED_FIELDS: list[pa.Field] = [
    pa.field("sequence_id", pa.string()),
    # Alignment features
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
    # k-mer features
    pa.field("kmer_frequencies", pa.list_(pa.float32(), 8000)),
    # Metadata
    pa.field("feature_version", pa.string()),
    pa.field("run_id", pa.string()),
    pa.field("created_at", pa.timestamp("us", tz="UTC")),
]

# Variable-size list (not fixed-size) because Parquet can't
# round-trip fixed-size lists with null entries.  Dimensionality
# is validated at load_embeddings() time.
_EMBEDDING_FIELD = pa.field("esm2_embedding", pa.list_(pa.float32()))

COMBINED_SCHEMA = pa.schema(
    _COMBINED_FIELDS[:12]  # through kmer_frequencies
    + [_EMBEDDING_FIELD]
    + _COMBINED_FIELDS[12:]  # metadata
)
"""Schema for combined features with ESM-2 embeddings."""

COMBINED_SCHEMA_NO_EMBEDDINGS = pa.schema(_COMBINED_FIELDS)
"""Schema for combined features without ESM-2 embeddings."""


# -- Validation ---------------------------------------------------------------


def _validate_id_agreement(
    align_ids: set[str],
    kmer_ids: set[str],
) -> None:
    """Raise ValueError if alignment and k-mer ID sets disagree."""
    if align_ids == kmer_ids:
        return

    only_align = align_ids - kmer_ids
    only_kmer = kmer_ids - align_ids
    samples_align = sorted(only_align)[:5]
    samples_kmer = sorted(only_kmer)[:5]

    msg = (
        f"sequence_id mismatch between alignment "
        f"({len(align_ids)} IDs) and k-mer "
        f"({len(kmer_ids)} IDs) features. "
        f"Only in alignment: {samples_align}. "
        f"Only in k-mer: {samples_kmer}."
    )
    raise ValueError(msg)


# -- Public API ---------------------------------------------------------------


def combine_features(
    alignment_features_path: Path,
    kmer_features_path: Path,
    output_path: Path,
    run_id: str,
    embeddings_path: Path | None = None,
    feature_version: str = "v1",
) -> Path:
    """Combine alignment, k-mer, and optional ESM-2 features.

    The alignment feature table is the master sequence list.
    Alignment and k-mer tables must have identical
    ``sequence_id`` sets (inner join). Embeddings, if provided,
    are left-joined — sequences without embeddings get null.

    Args:
        alignment_features_path: Parquet from
            :func:`~.alignment_features.extract_alignment_features`.
        kmer_features_path: Parquet from
            :func:`~.kmer_features.extract_kmer_features`.
        output_path: Where to write the combined Parquet.
        run_id: Run identifier for the metadata column.
        embeddings_path: Optional path to pre-computed ESM-2
            embeddings Parquet.
        feature_version: Schema version string.

    Returns:
        *output_path* for convenience.

    Raises:
        ValueError: If alignment and k-mer ``sequence_id`` sets
            disagree.
    """
    alignment = pq.read_table(str(alignment_features_path))
    kmer = pq.read_table(str(kmer_features_path))

    # -- Validate ID agreement -------------------------------------------
    align_ids = set(alignment.column("sequence_id").to_pylist())
    kmer_ids = set(kmer.column("sequence_id").to_pylist())
    _validate_id_agreement(align_ids, kmer_ids)

    # -- Load optional embeddings ----------------------------------------
    embeddings: pa.Table | None = None
    if embeddings_path is not None:
        embeddings = load_embeddings(embeddings_path)

    # -- Join alignment + kmer via DuckDB --------------------------------
    conn = duckdb.connect()
    try:
        conn.register("alignment", alignment)
        conn.register("kmer", kmer)

        acols = ", ".join(f"a.{c}" for c in _ALIGNMENT_COLS)
        sql = f"""
            SELECT
                {acols},
                k.kmer_frequencies
            FROM alignment a
            INNER JOIN kmer k USING (sequence_id)
            ORDER BY a.sequence_id
        """
        result = conn.execute(sql).arrow().read_all()
    finally:
        conn.close()

    # -- Left-join embeddings in PyArrow ----------------------------------
    # Parquet round-trips of fixed-size lists with null entries are
    # broken in some PyArrow versions (size-0 entries instead of
    # proper nulls).  We build the column using from_buffers with
    # an explicit validity bitmap.
    if embeddings is not None:
        emb_lookup: dict[str, list[float]] = {}
        emb_ids = embeddings.column("sequence_id").to_pylist()
        emb_vecs = embeddings.column("embedding")
        for i, sid in enumerate(emb_ids):
            emb_lookup[sid] = emb_vecs[i].as_py()

        combined_ids = result.column("sequence_id").to_pylist()
        emb_col: list[list[float] | None] = [
            emb_lookup.get(sid) for sid in combined_ids
        ]
        emb_array = pa.array(emb_col, type=pa.list_(pa.float32()))
        result = result.append_column("esm2_embedding", emb_array)

    # -- Add metadata columns --------------------------------------------
    n = result.num_rows
    now = datetime.now(tz=UTC)
    result = result.append_column(
        "feature_version",
        pa.array([feature_version] * n, type=pa.string()),
    )
    result = result.append_column(
        "run_id",
        pa.array([run_id] * n, type=pa.string()),
    )
    result = result.append_column(
        "created_at",
        pa.array([now] * n, type=pa.timestamp("us", tz="UTC")),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(result, str(output_path))

    has_emb = embeddings is not None
    logger.info(
        "features_combined",
        total_sequences=n,
        has_embeddings=has_emb,
        feature_version=feature_version,
        output=str(output_path),
    )

    return output_path
