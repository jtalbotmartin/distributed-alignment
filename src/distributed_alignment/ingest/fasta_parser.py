"""Streaming FASTA parser yielding validated ProteinSequence objects.

Parses FASTA files as a generator — never loads the entire file into memory.
Each sequence is validated against the ProteinSequence model on yield.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

from distributed_alignment.models import ProteinSequence

logger = structlog.get_logger(component="ingest")

# Default maximum sequence length before a warning is logged
DEFAULT_MAX_LENGTH = 100_000


def parse_fasta(
    path: Path,
    *,
    max_length: int = DEFAULT_MAX_LENGTH,
) -> Generator[ProteinSequence, None, None]:
    """Parse a FASTA file, yielding validated ProteinSequence objects.

    Args:
        path: Path to the FASTA file.
        max_length: Sequences exceeding this length trigger a warning
            but are still yielded. Set to 0 to disable the warning.

    Yields:
        ProteinSequence for each valid entry in the file.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If a line is encountered before any header,
            if a header has no sequence ID, or if a sequence contains
            invalid amino acid characters.
    """
    header: str | None = None
    seq_id: str | None = None
    sequence_lines: list[str] = []
    line_number = 0

    with path.open() as fh:
        for raw_line in fh:
            line_number += 1
            line = raw_line.rstrip("\n\r")

            if not line or line.isspace():
                continue

            if line.startswith(">"):
                # Yield the previous sequence if we have one
                if header is not None and seq_id is not None:
                    yield _build_sequence(
                        seq_id,
                        header,
                        sequence_lines,
                        line_number - 1,
                        max_length,
                    )

                # Parse the new header
                header = line[1:].strip()
                if not header:
                    msg = (
                        f"Line {line_number}: empty FASTA header "
                        f"(no text after '>')"
                    )
                    raise ValueError(msg)
                seq_id = header.split()[0]
                sequence_lines = []

            else:
                if header is None:
                    msg = (
                        f"Line {line_number}: sequence data found "
                        f"before any FASTA header line (expected '>')"
                    )
                    raise ValueError(msg)
                sequence_lines.append(line.strip())

    # Yield the last sequence in the file
    if header is not None and seq_id is not None:
        yield _build_sequence(
            seq_id, header, sequence_lines, line_number, max_length
        )


def _build_sequence(
    seq_id: str,
    description: str,
    sequence_lines: list[str],
    line_number: int,
    max_length: int,
) -> ProteinSequence:
    """Construct and validate a ProteinSequence from accumulated lines.

    Args:
        seq_id: Parsed sequence identifier.
        description: Full header line text (without '>').
        sequence_lines: Accumulated sequence data lines.
        line_number: Current line number (for error context).
        max_length: Warn if sequence exceeds this length.

    Returns:
        A validated ProteinSequence.

    Raises:
        ValueError: If the sequence is empty or contains invalid characters.
    """
    sequence = "".join(sequence_lines)

    if not sequence:
        msg = (
            f"Line {line_number}: sequence '{seq_id}' has no "
            f"amino acid data"
        )
        raise ValueError(msg)

    length = len(sequence)

    if max_length > 0 and length > max_length:
        logger.warning(
            "sequence_exceeds_max_length",
            sequence_id=seq_id,
            length=length,
            max_length=max_length,
            line_number=line_number,
        )

    try:
        return ProteinSequence(
            id=seq_id,
            description=description,
            sequence=sequence,
            length=length,
        )
    except ValueError as exc:
        msg = f"Line {line_number}: sequence '{seq_id}': {exc}"
        raise ValueError(msg) from exc
