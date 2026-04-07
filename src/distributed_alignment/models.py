"""Shared Pydantic models — data contracts for the distributed-alignment pipeline.

Every pipeline stage boundary uses these models for validation.
"""

from __future__ import annotations

from datetime import datetime  # noqa: TCH003 - Pydantic needs this at runtime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, Field, field_validator

# Standard 20 amino acids + common ambiguity codes
VALID_AMINO_ACIDS: frozenset[str] = frozenset(
    "ACDEFGHIKLMNPQRSTVWY"  # standard 20
    "BJOUXZ"  # ambiguity codes
    "*"  # stop codon (sometimes present)
)


class ProteinSequence(BaseModel):
    """A single protein sequence parsed from a FASTA file."""

    id: str = Field(description="Sequence identifier from FASTA header")
    description: str = Field(description="Full FASTA header line")
    sequence: str = Field(description="Amino acid sequence")
    length: int = Field(description="Sequence length, computed from sequence")

    @field_validator("sequence")
    @classmethod
    def validate_amino_acids(cls, v: str) -> str:
        """Validate that sequence contains only valid amino acid characters."""
        upper = v.upper()
        invalid = set(upper) - VALID_AMINO_ACIDS
        if invalid:
            msg = f"Invalid amino acid characters: {sorted(invalid)}"
            raise ValueError(msg)
        return upper

    @field_validator("length")
    @classmethod
    def validate_length(cls, v: int) -> int:
        """Validate that sequence length is positive."""
        if v <= 0:
            msg = "Sequence length must be positive"
            raise ValueError(msg)
        return v


class ChunkEntry(BaseModel):
    """Metadata for a single chunk within a manifest."""

    chunk_id: str
    num_sequences: int
    parquet_path: str
    content_checksum: str


class ChunkManifest(BaseModel):
    """Manifest cataloguing all chunks produced during ingestion."""

    run_id: str
    input_files: list[str]
    total_sequences: int
    num_chunks: int
    chunk_size_target: int
    chunks: list[ChunkEntry]
    created_at: datetime
    chunking_strategy: Literal["deterministic_hash", "sequential"]


class WorkPackageState(StrEnum):
    """States in the work package state machine."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    POISONED = "POISONED"


class WorkPackage(BaseModel):
    """A single unit of alignment work: one query chunk × one reference chunk."""

    package_id: str
    query_chunk_id: str
    ref_chunk_id: str
    state: WorkPackageState = WorkPackageState.PENDING
    claimed_by: str | None = None
    claimed_at: datetime | None = None
    heartbeat_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    attempt: int = 0
    max_attempts: int = 3
    error_history: list[str] = Field(default_factory=list)


class MergedHit(BaseModel):
    """A single alignment hit after merging and global ranking."""

    query_id: str
    subject_id: str
    percent_identity: float
    alignment_length: int
    mismatches: int
    gap_opens: int
    query_start: int
    query_end: int
    subject_start: int
    subject_end: int
    evalue: float
    bitscore: float
    global_rank: int
    query_chunk_id: str
    ref_chunk_id: str


class FeatureRow(BaseModel):
    """ML-ready feature row for a single query sequence."""

    sequence_id: str

    # Stream A: alignment-derived features
    hit_count: int
    mean_percent_identity: float
    max_percent_identity: float
    mean_evalue_log10: float
    mean_alignment_length: float
    std_alignment_length: float
    best_hit_query_coverage: float
    taxonomic_entropy: float
    num_phyla: int
    num_kingdoms: int

    # Metadata
    feature_version: str
    run_id: str
    created_at: datetime
