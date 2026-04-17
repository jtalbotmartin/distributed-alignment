#!/usr/bin/env python3
"""Compute ESM-2 embeddings for protein sequences.

Reads a FASTA file, runs the ESM-2 language model in batches, and
writes multi-scale pooled embeddings (mean + max + std, 960-dim)
to Parquet.

Requires the ``embeddings`` optional extra::

    uv sync --extra embeddings

Regenerate the Tier 1 fixture::

    uv run --extra embeddings python scripts/compute_embeddings.py \\
        --fasta tests/fixtures/metagenome_queries.fasta \\
        --output tests/fixtures/query_embeddings.parquet \\
        --run-id fixture-tier1

Uses ``esm2_t6_8M_UR50D`` (8M parameters, 320-dim per-residue,
960-dim after multi-scale pooling, 6 transformer layers).  Fast
enough for CPU; uses CUDA when available.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path  # noqa: TCH003 — used at runtime
from typing import Annotated

try:
    import esm
    import torch
except ImportError as e:
    raise ImportError(
        "ESM-2 embeddings require the 'embeddings' extra. "
        "Install with: uv sync --extra embeddings"
    ) from e

import pyarrow as pa
import pyarrow.parquet as pq
import structlog
import typer

from distributed_alignment.features.embedding_features import (
    EMBEDDING_DIM,
    EMBEDDING_SCHEMA,
    ESM_HIDDEN_DIM,
    MODEL_NAME,
)
from distributed_alignment.ingest.fasta_parser import parse_fasta

logger = structlog.get_logger(__name__)

app = typer.Typer(add_completion=False)

MAX_TOKENS: int = 1022  # ESM-2 context = 1024, minus BOS + EOS


def _compute_embeddings(
    sequences: list[tuple[str, str]],
    batch_size: int = 32,
) -> tuple[list[str], list[list[float]]]:
    """Run ESM-2 on a list of (id, sequence) pairs.

    Returns:
        Tuple of (sequence_ids, embedding_vectors).
    """
    logger.info(
        "loading_model",
        model=MODEL_NAME,
    )
    model, alphabet = getattr(esm.pretrained, MODEL_NAME)()
    batch_converter = alphabet.get_batch_converter()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    model.eval()

    logger.info(
        "model_loaded",
        model=MODEL_NAME,
        device=str(device),
        num_sequences=len(sequences),
    )

    all_ids: list[str] = []
    all_embeddings: list[list[float]] = []

    for batch_start in range(0, len(sequences), batch_size):
        batch = sequences[batch_start : batch_start + batch_size]
        t0 = time.monotonic()

        # Truncate sequences exceeding the context window
        processed: list[tuple[str, str]] = []
        for sid, seq in batch:
            if len(seq) > MAX_TOKENS:
                logger.warning(
                    "sequence_truncated",
                    sequence_id=sid,
                    original_length=len(seq),
                    truncated_to=MAX_TOKENS,
                )
                seq = seq[:MAX_TOKENS]
            processed.append((sid, seq))

        _, _, batch_tokens = batch_converter([(sid, seq) for sid, seq in processed])
        batch_tokens = batch_tokens.to(device)

        with torch.no_grad():
            results = model(
                batch_tokens,
                repr_layers=[model.num_layers],
                return_contacts=False,
            )

        # Extract final-layer representations
        representations = results["representations"][model.num_layers]  # (B, L, 320)

        for i, (sid, seq) in enumerate(processed):
            # Multi-scale pooling over residue positions,
            # excluding BOS (position 0) and EOS/padding.
            seq_len = len(seq)
            # Positions 1..seq_len are the residue tokens
            token_repr = representations[
                i, 1 : seq_len + 1, :
            ]  # (seq_len, 320)

            if token_repr.shape[0] == 0:
                # Pathologically short sequence
                logger.warning(
                    "empty_representation",
                    sequence_id=sid,
                )
                embedding = [0.0] * EMBEDDING_DIM
            else:
                mean_pool = token_repr.mean(dim=0)
                max_pool = token_repr.max(dim=0).values
                std_pool = (
                    token_repr.std(dim=0)
                    if token_repr.shape[0] > 1
                    else torch.zeros(ESM_HIDDEN_DIM)
                )
                embedding = (
                    torch.cat([mean_pool, max_pool, std_pool])
                    .cpu()
                    .tolist()
                )

            all_ids.append(sid)
            all_embeddings.append(embedding)

        elapsed = time.monotonic() - t0
        logger.info(
            "batch_complete",
            batch_start=batch_start,
            batch_size=len(batch),
            elapsed_seconds=round(elapsed, 2),
            progress=f"{min(batch_start + batch_size, len(sequences))}"
            f"/{len(sequences)}",
        )

    return all_ids, all_embeddings


@app.command()
def main(
    fasta: Annotated[Path, typer.Option(help="Input FASTA file")],
    output: Annotated[Path, typer.Option(help="Output Parquet path")],
    batch_size: Annotated[int, typer.Option(help="Sequences per batch")] = 32,
    run_id: Annotated[str, typer.Option(help="Pipeline run identifier")] = "",
    feature_version: Annotated[str, typer.Option(help="Feature schema version")] = "v1",
) -> None:
    """Compute ESM-2 embeddings for all sequences in a FASTA file."""
    from datetime import UTC, datetime

    if not fasta.exists():
        logger.error("fasta_not_found", path=str(fasta))
        sys.exit(1)

    # Parse FASTA
    sequences: list[tuple[str, str]] = []
    for protein in parse_fasta(fasta, max_length=0):
        sequences.append((protein.id, protein.sequence))

    logger.info(
        "fasta_parsed",
        path=str(fasta),
        num_sequences=len(sequences),
    )

    if not sequences:
        # Write empty Parquet with correct schema
        empty = pa.table(
            {f.name: pa.array([], type=f.type) for f in EMBEDDING_SCHEMA},
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(empty, str(output))
        logger.info("empty_output_written", output=str(output))
        return

    seq_ids, embeddings = _compute_embeddings(sequences, batch_size=batch_size)

    n = len(seq_ids)
    now = datetime.now(tz=UTC)

    table = pa.table(
        {
            "sequence_id": pa.array(seq_ids, type=pa.string()),
            "embedding": pa.FixedSizeListArray.from_arrays(
                pa.array(
                    [v for vec in embeddings for v in vec],
                    type=pa.float32(),
                ),
                list_size=EMBEDDING_DIM,
            ),
            "feature_version": pa.array([feature_version] * n, type=pa.string()),
            "run_id": pa.array([run_id] * n, type=pa.string()),
            "created_at": pa.array(
                [now] * n,
                type=pa.timestamp("us", tz="UTC"),
            ),
        },
        schema=EMBEDDING_SCHEMA,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(output))

    logger.info(
        "embeddings_written",
        output=str(output),
        num_sequences=n,
        embedding_dim=EMBEDDING_DIM,
    )


if __name__ == "__main__":
    app()
