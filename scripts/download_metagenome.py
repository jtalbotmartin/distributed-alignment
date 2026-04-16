#!/usr/bin/env python3
"""Download real metagenomic data for pipeline testing (Tier 2).

Downloads:
  1. Predicted proteins from a soil metagenome via MGnify
  2. Full Swiss-Prot as reference database

Saves to data/metagenome/ (gitignored). ~300MB total.

Usage:
    python scripts/download_metagenome.py
"""

from __future__ import annotations

import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "metagenome"

# MGnify soil metagenome — Earth Microbiome Project grassland soil sample
# This analysis has predicted CDS (protein sequences) available
MGNIFY_ANALYSIS = "MGYA00585182"
MGNIFY_DOWNLOAD_URL = (
    f"https://www.ebi.ac.uk/metagenomics/api/v1/analyses"
    f"/{MGNIFY_ANALYSIS}/downloads"
)

SWISSPROT_URL = (
    "https://rest.uniprot.org/uniprotkb/stream"
    "?format=fasta&query=(reviewed:true)"
)


def download_file(
    url: str,
    output: Path,
    *,
    description: str = "",
    max_retries: int = 3,
) -> bool:
    """Download a URL to a file with retry logic.

    Returns True on success, False on failure.
    """
    for attempt in range(max_retries):
        try:
            print(f"  Downloading {description or url}...")
            resp = urllib.request.urlopen(url, timeout=300)  # noqa: S310
            data = resp.read()
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(data)
            size_mb = len(data) / (1024 * 1024)
            print(f"  Saved {output} ({size_mb:.1f} MB)")
            return True
        except (urllib.error.URLError, TimeoutError) as exc:
            wait = 2 ** (attempt + 1)
            print(f"  Retry {attempt + 1}/{max_retries}: {exc} (waiting {wait}s)")
            time.sleep(wait)

    print(f"  ERROR: Failed to download after {max_retries} retries")
    return False


def download_mgnify_proteins() -> bool:
    """Download predicted protein sequences from MGnify."""
    import json

    print(f"Fetching MGnify download links for {MGNIFY_ANALYSIS}...")

    try:
        resp = urllib.request.urlopen(MGNIFY_DOWNLOAD_URL, timeout=30)  # noqa: S310
        downloads = json.loads(resp.read())
    except Exception as exc:
        print(f"  ERROR: Could not fetch MGnify downloads: {exc}")
        print("  Falling back to direct URL...")

        # Fallback: try the standard predicted CDS URL pattern
        fallback_url = (
            f"https://www.ebi.ac.uk/metagenomics/api/v1/analyses"
            f"/{MGNIFY_ANALYSIS}/file"
            f"/{MGNIFY_ANALYSIS}_predicted_CDS.faa.gz"
        )
        return download_file(
            fallback_url,
            DATA_DIR / "queries.fasta.gz",
            description="MGnify predicted proteins (fallback)",
        )

    # Find the predicted CDS file in the downloads list
    for item in downloads.get("data", []):
        attrs = item.get("attributes", {})
        desc_type = attrs.get("description", {}).get("label", "")
        is_cds = "Predicted CDS" in desc_type
        is_fasta = attrs.get("file_format", {}).get("name") == "FASTA"
        if is_cds and is_fasta:
            dl_url = item["links"]["self"]
            return download_file(
                dl_url,
                DATA_DIR / "queries.fasta",
                description=f"MGnify {MGNIFY_ANALYSIS} predicted proteins",
            )

    print("  WARNING: Could not find predicted CDS download in MGnify API response")
    print("  You may need to download manually from:")
    print(f"  https://www.ebi.ac.uk/metagenomics/analyses/{MGNIFY_ANALYSIS}")
    return False


def download_swissprot() -> bool:
    """Download full Swiss-Prot as reference database (~250MB)."""
    print("Downloading full Swiss-Prot (~570K sequences, ~250MB)...")
    return download_file(
        SWISSPROT_URL,
        DATA_DIR / "swissprot_full.fasta",
        description="Swiss-Prot (reviewed)",
    )


CLINICAL_DIR = Path(__file__).parent.parent / "data" / "clinical"

# MGnify human gut metagenome — HMP study
MGNIFY_GUT_ANALYSIS = "MGYA00593746"


def download_gut_metagenome() -> bool:
    """Download predicted proteins from a human gut metagenome."""
    print("Downloading gut metagenome proteins from MGnify...")

    output = CLINICAL_DIR / "gut_metagenome.fasta"
    if output.exists():
        print(f"  Skipping (already exists: {output})")
        return True

    CLINICAL_DIR.mkdir(parents=True, exist_ok=True)

    url = (
        f"https://www.ebi.ac.uk/metagenomics/api/v1/analyses"
        f"/{MGNIFY_GUT_ANALYSIS}/file"
        f"/{MGNIFY_GUT_ANALYSIS}_predicted_CDS.faa.gz"
    )
    return download_file(
        url,
        output,
        description=f"MGnify gut metagenome {MGNIFY_GUT_ANALYSIS}",
    )


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("=== Tier 2: Real Metagenome Datasets ===\n")

    print("--- Soil metagenome ---")
    ok_soil = download_mgnify_proteins()
    print()

    print("--- Clinical gut metagenome ---")
    ok_gut = download_gut_metagenome()
    print()

    print("--- Reference database ---")
    ok_ref = download_swissprot()

    print()
    if ok_soil and ok_ref:
        print("Data saved to data/metagenome/")
        print("  queries.fasta        — soil metagenomic proteins")
        print("  swissprot_full.fasta — Swiss-Prot reference")
    if ok_gut:
        print("Data saved to data/clinical/")
        print("  gut_metagenome.fasta — gut metagenomic proteins")
    if not (ok_soil and ok_ref):
        print("\nSome downloads failed. Check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
