"""Sequence ingestion: FASTA parsing and deterministic chunking."""

from distributed_alignment.ingest.chunker import chunk_sequences
from distributed_alignment.ingest.fasta_parser import parse_fasta

__all__ = ["chunk_sequences", "parse_fasta"]
