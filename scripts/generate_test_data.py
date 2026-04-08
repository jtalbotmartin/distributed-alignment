#!/usr/bin/env python3
"""Generate synthetic protein FASTA files for testing.

Produces small query and reference datasets with valid amino acid sequences.
No network access required.

Usage:
    python scripts/generate_test_data.py [output_dir]
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

AMINO_ACIDS = "ACDEFGHIKLMNPQRSTVWY"


def generate_fasta(
    output_path: Path,
    *,
    num_sequences: int,
    prefix: str,
    min_length: int = 50,
    max_length: int = 500,
    seed: int = 42,
) -> None:
    """Generate a synthetic FASTA file with random protein sequences.

    Args:
        output_path: Path to write the FASTA file.
        num_sequences: Number of sequences to generate.
        prefix: Prefix for sequence IDs (e.g. "query" or "ref").
        min_length: Minimum sequence length.
        max_length: Maximum sequence length.
        seed: Random seed for reproducibility.
    """
    rng = random.Random(seed)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w") as f:
        for i in range(num_sequences):
            seq_id = f"{prefix}_{i:05d}"
            length = rng.randint(min_length, max_length)
            sequence = "".join(rng.choices(AMINO_ACIDS, k=length))
            f.write(f">{seq_id} Synthetic protein {i}\n")
            # Write sequence in 80-char lines (standard FASTA convention)
            for start in range(0, length, 80):
                f.write(sequence[start : start + 80] + "\n")


def main() -> None:
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("test_data")
    output_dir.mkdir(parents=True, exist_ok=True)

    generate_fasta(
        output_dir / "queries.fasta",
        num_sequences=100,
        prefix="query",
        seed=42,
    )
    generate_fasta(
        output_dir / "reference.fasta",
        num_sequences=500,
        prefix="ref",
        seed=123,
    )

    print(f"Generated test data in {output_dir}/")
    print("  queries.fasta:   100 sequences")
    print("  reference.fasta: 500 sequences")


if __name__ == "__main__":
    main()
