"""Deterministic hash-based sequence chunker.

Assigns each sequence to a chunk via ``SHA-256(sequence_id) % num_chunks``,
writes each chunk as a Parquet file, and produces a JSON manifest.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from distributed_alignment.models import ChunkEntry, ChunkManifest

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from distributed_alignment.models import ProteinSequence

logger = structlog.get_logger(component="chunker")

# Parquet schema for chunk files — matches TDD section 3.1
CHUNK_SCHEMA = pa.schema(
    [
        pa.field("chunk_id", pa.string()),
        pa.field("sequence_id", pa.string()),
        pa.field("description", pa.string()),
        pa.field("sequence", pa.string()),
        pa.field("length", pa.int32()),
        pa.field("content_hash", pa.string()),
    ]
)


def assign_chunk(sequence_id: str, num_chunks: int) -> int:
    """Deterministically assign a sequence to a chunk index.

    Uses SHA-256 of the sequence ID, not Python's built-in ``hash()``,
    which is randomised per process.

    Args:
        sequence_id: The sequence identifier to hash.
        num_chunks: Total number of chunks.

    Returns:
        Chunk index in ``[0, num_chunks)``.
    """
    digest = hashlib.sha256(sequence_id.encode()).hexdigest()
    return int(digest, 16) % num_chunks


def sequence_content_hash(sequence: str) -> str:
    """Compute the SHA-256 content hash of a protein sequence."""
    return hashlib.sha256(sequence.encode()).hexdigest()


def file_checksum(path: Path) -> str:
    """Compute the SHA-256 checksum of a file's contents."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8192), b""):
            h.update(block)
    return f"sha256:{h.hexdigest()}"


def chunk_sequences(
    sequences: Iterable[ProteinSequence],
    *,
    num_chunks: int,
    output_dir: Path,
    chunk_prefix: str,
    run_id: str,
    input_files: list[str],
) -> ChunkManifest:
    """Chunk an iterable of protein sequences into Parquet files.

    Args:
        sequences: Iterable of validated ProteinSequence objects
            (e.g. from ``parse_fasta``).
        num_chunks: Number of chunks to distribute sequences across.
        output_dir: Directory to write chunk Parquet files into.
            Created if it doesn't exist.
        chunk_prefix: Prefix for chunk IDs (e.g. ``"q"`` for queries,
            ``"r"`` for references).
        run_id: Pipeline run identifier.
        input_files: List of source file names for manifest metadata.

    Returns:
        A ChunkManifest describing all produced chunks.
    """
    if num_chunks <= 0:
        msg = "num_chunks must be positive"
        raise ValueError(msg)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Accumulate sequences per chunk bucket
    buckets: dict[int, list[ProteinSequence]] = {i: [] for i in range(num_chunks)}
    total_sequences = 0

    for seq in sequences:
        idx = assign_chunk(seq.id, num_chunks)
        buckets[idx].append(seq)
        total_sequences += 1

    logger.info(
        "chunking_complete",
        total_sequences=total_sequences,
        num_chunks=num_chunks,
        run_id=run_id,
    )

    # Write each non-empty bucket as a Parquet file
    chunk_entries: list[ChunkEntry] = []

    for idx in range(num_chunks):
        chunk_id = f"{chunk_prefix}{idx:03d}"
        # Sort by sequence ID for deterministic row ordering
        bucket = sorted(buckets[idx], key=lambda s: s.id)

        if not bucket:
            logger.debug(
                "empty_chunk_skipped",
                chunk_id=chunk_id,
            )
            continue

        parquet_path = output_dir / f"chunk_{chunk_id}.parquet"

        table = pa.table(
            {
                "chunk_id": [chunk_id] * len(bucket),
                "sequence_id": [s.id for s in bucket],
                "description": [s.description for s in bucket],
                "sequence": [s.sequence for s in bucket],
                "length": [s.length for s in bucket],
                "content_hash": [sequence_content_hash(s.sequence) for s in bucket],
            },
            schema=CHUNK_SCHEMA,
        )

        pq.write_table(table, parquet_path)

        entry = ChunkEntry(
            chunk_id=chunk_id,
            num_sequences=len(bucket),
            parquet_path=str(parquet_path),
            content_checksum=file_checksum(parquet_path),
        )
        chunk_entries.append(entry)

        logger.debug(
            "chunk_written",
            chunk_id=chunk_id,
            num_sequences=len(bucket),
            path=str(parquet_path),
        )

    manifest = ChunkManifest(
        run_id=run_id,
        input_files=input_files,
        total_sequences=total_sequences,
        num_chunks=num_chunks,
        chunk_size_target=max(1, total_sequences // num_chunks)
        if total_sequences > 0
        else 0,
        chunks=chunk_entries,
        created_at=datetime.now(tz=UTC),
        chunking_strategy="deterministic_hash",
    )

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest.model_dump(mode="json"), indent=2))

    logger.info(
        "manifest_written",
        path=str(manifest_path),
        num_chunk_files=len(chunk_entries),
    )

    return manifest
