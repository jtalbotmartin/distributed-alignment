#!/usr/bin/env python3
"""Download large datasets for stress-testing the distributed pipeline (Tier 3).

Downloads multiple MGnify metagenomic protein sets for a combined
100K-500K query sequences, plus the full UniRef50 as a large reference.

Saves to data/stress_test/ (gitignored). ~1-5GB total.

Usage:
    python scripts/download_stress_test.py
"""

from __future__ import annotations

from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "stress_test"

# Multiple MGnify soil metagenome analyses for a large combined query set
MGNIFY_ANALYSES = [
    "MGYA00585182",  # EMP grassland soil
    "MGYA00585174",  # EMP forest soil
    "MGYA00585186",  # EMP agricultural soil
]

UNIREF50_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot"
    "/uniref/uniref50/uniref50.fasta.gz"
)


def main() -> None:
    from scripts.download_metagenome import download_file

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("=== Tier 3: Stress Test Dataset ===\n")
    print("This downloads several GB of data. Make sure you have disk space.\n")

    # Download multiple metagenomes
    for analysis in MGNIFY_ANALYSES:
        output = DATA_DIR / f"{analysis}_proteins.fasta"
        if output.exists():
            print(f"  Skipping {analysis} (already downloaded)")
            continue

        url = (
            f"https://www.ebi.ac.uk/metagenomics/api/v1/analyses"
            f"/{analysis}/file/{analysis}_predicted_CDS.faa.gz"
        )
        download_file(
            url,
            output,
            description=f"MGnify {analysis}",
        )
        print()

    # Download UniRef50 as large reference
    uniref_output = DATA_DIR / "uniref50.fasta.gz"
    if uniref_output.exists():
        print("  Skipping UniRef50 (already downloaded)")
    else:
        print("Downloading UniRef50 reference (~5GB compressed)...")
        print("  This will take a while...")
        download_file(
            UNIREF50_URL,
            uniref_output,
            description="UniRef50",
        )

    print()
    print("Done. Data saved to data/stress_test/")
    print("  To run the pipeline:")
    print("  1. Concatenate: cat data/stress_test/*_proteins.fasta"
          " > data/stress_test/queries.fasta")
    print("  2. Decompress:  gunzip data/stress_test/uniref50.fasta.gz")
    print("  3. Run:         make ingest ARGS='--queries"
          " data/stress_test/queries.fasta"
          " --reference data/stress_test/uniref50.fasta"
          " --output-dir work/'")


if __name__ == "__main__":
    main()
