"""Stream B: k-mer frequency feature extraction.

Computes the frequency vector of overlapping amino-acid 3-mers
(8,000 dimensions) for each query sequence.  Zero additional
dependencies — pure Python counting over the 20 standard amino
acids.
"""

from __future__ import annotations

import itertools
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path  # noqa: TCH003 — used at runtime

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger(__name__)

# -- Vocabulary --------------------------------------------------------------

AMINO_ACIDS: str = "ACDEFGHIKLMNPQRSTVWY"
_AA_SET: frozenset[str] = frozenset(AMINO_ACIDS)

KMER_VOCABULARY: tuple[str, ...] = tuple(
    "".join(tri) for tri in itertools.product(AMINO_ACIDS, repeat=3)
)

_K: int = 3
_VOCAB_SIZE: int = len(KMER_VOCABULARY)  # 8000

# -- Schema ------------------------------------------------------------------

KMER_SCHEMA = pa.schema(
    [
        pa.field("sequence_id", pa.string()),
        pa.field(
            "kmer_frequencies",
            pa.list_(pa.float32(), _VOCAB_SIZE),
        ),
        pa.field("feature_version", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
    ]
)

# -- Per-sequence counting ---------------------------------------------------


def _kmer_frequencies(sequence: str) -> list[float]:
    """Compute normalised 3-mer frequency vector for *sequence*.

    Non-standard residues (anything outside the 20 standard amino
    acids) cause the containing 3-mer to be skipped entirely.
    Sequences that yield zero valid 3-mers return an all-zero
    vector.
    """
    upper = sequence.upper()
    counts: Counter[str] = Counter()
    total = 0

    for i in range(len(upper) - _K + 1):
        tri = upper[i : i + _K]
        if tri[0] in _AA_SET and tri[1] in _AA_SET and tri[2] in _AA_SET:
            counts[tri] += 1
            total += 1

    if total == 0:
        return [0.0] * _VOCAB_SIZE

    inv = 1.0 / total
    return [counts.get(kmer, 0) * inv for kmer in KMER_VOCABULARY]


# -- Public API --------------------------------------------------------------


def extract_kmer_features(
    chunks_dir: Path,
    output_path: Path,
    run_id: str = "",
    *,
    feature_version: str = "v1",
) -> Path:
    """Compute 3-mer frequency features for all query sequences.

    Reads every query sequence from ``chunks_dir/queries/*.parquet``,
    computes the 8,000-dimensional frequency vector, and writes one
    row per sequence to *output_path*.

    Args:
        chunks_dir: Pipeline work directory containing
            ``chunks/queries/*.parquet``.
        output_path: Where to write the k-mer feature Parquet.
        run_id: Pipeline run identifier (metadata column).
        feature_version: Feature schema version (metadata column).

    Returns:
        *output_path* for convenience.
    """
    query_dir = chunks_dir / "queries"
    if not query_dir.exists():
        msg = f"Query chunks directory not found: {query_dir}"
        raise FileNotFoundError(msg)

    chunk_files = sorted(query_dir.glob("*.parquet"))

    seq_ids: list[str] = []
    freq_vectors: list[list[float]] = []

    for fpath in chunk_files:
        table = pq.read_table(str(fpath), columns=["sequence_id", "sequence"])
        for sid, seq in zip(
            table.column("sequence_id").to_pylist(),
            table.column("sequence").to_pylist(),
            strict=True,
        ):
            seq_ids.append(sid)
            freq_vectors.append(_kmer_frequencies(seq))

    n = len(seq_ids)
    now = datetime.now(tz=UTC)

    result = pa.table(
        {
            "sequence_id": pa.array(seq_ids, type=pa.string()),
            "kmer_frequencies": pa.FixedSizeListArray.from_arrays(
                pa.array(
                    [v for vec in freq_vectors for v in vec],
                    type=pa.float32(),
                ),
                list_size=_VOCAB_SIZE,
            ),
            "feature_version": pa.array([feature_version] * n, type=pa.string()),
            "run_id": pa.array([run_id] * n, type=pa.string()),
            "created_at": pa.array([now] * n, type=pa.timestamp("us", tz="UTC")),
        },
        schema=KMER_SCHEMA,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(result, str(output_path))

    logger.info(
        "kmer_features_extracted",
        total_sequences=n,
        vocabulary_size=_VOCAB_SIZE,
        output=str(output_path),
    )

    return output_path
