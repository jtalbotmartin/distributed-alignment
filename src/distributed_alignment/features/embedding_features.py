"""Stream C: ESM-2 protein language model embeddings (loader).

Reads pre-computed ESM-2 embeddings from Parquet.  This module
has **no** ``torch`` or ``fair-esm`` dependency — it is pure
PyArrow and can be imported in the default environment.

The compute step lives in ``scripts/compute_embeddings.py``,
which is a separate entry point requiring the ``embeddings``
optional extra.
"""

from __future__ import annotations

from pathlib import Path  # noqa: TCH003 — used at runtime

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger(__name__)

# -- Constants ---------------------------------------------------------------

ESM_HIDDEN_DIM: int = 320
"""Hidden dimensionality of ESM-2 (esm2_t6_8M_UR50D) per-residue representations."""

EMBEDDING_DIM: int = ESM_HIDDEN_DIM * 3  # 960
"""Dimensionality of the multi-scale pooled embedding (mean + max + std)."""

MODEL_NAME: str = "esm2_t6_8M_UR50D"
"""ESM-2 model variant used for embedding computation."""

EMBEDDING_SCHEMA = pa.schema(
    [
        pa.field("sequence_id", pa.string()),
        pa.field(
            "embedding",
            pa.list_(pa.float32(), EMBEDDING_DIM),
        ),
        pa.field("feature_version", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
    ]
)

# -- Public API --------------------------------------------------------------


def load_embeddings(parquet_path: Path) -> pa.Table:
    """Load pre-computed ESM-2 embeddings from Parquet.

    Args:
        parquet_path: Path to the embeddings Parquet file.

    Returns:
        PyArrow table conforming to :data:`EMBEDDING_SCHEMA`.

    Raises:
        FileNotFoundError: If *parquet_path* does not exist.
        ValueError: If the loaded schema does not match
            :data:`EMBEDDING_SCHEMA` (e.g. wrong embedding
            dimension).
    """
    if not parquet_path.exists():
        msg = f"Embeddings file not found: {parquet_path}"
        raise FileNotFoundError(msg)

    table = pq.read_table(str(parquet_path))

    # Validate the embedding column exists and has the right shape.
    if "embedding" not in table.schema.names:
        msg = (
            "Embeddings Parquet missing 'embedding' column. "
            f"Columns found: {table.schema.names}"
        )
        raise ValueError(msg)

    emb_type = table.schema.field("embedding").type
    expected_type = pa.list_(pa.float32(), EMBEDDING_DIM)
    if emb_type != expected_type:
        msg = (
            f"Embedding column has type {emb_type}, "
            f"expected {expected_type} "
            f"(fixed-size list of {EMBEDDING_DIM} float32)"
        )
        raise ValueError(msg)

    logger.info(
        "embeddings_loaded",
        path=str(parquet_path),
        num_sequences=len(table),
        embedding_dim=EMBEDDING_DIM,
    )

    return table
