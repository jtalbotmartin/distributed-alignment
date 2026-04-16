#!/usr/bin/env python3
"""Download taxonomically diverse Swiss-Prot fixtures for testing.

Creates:
  tests/fixtures/metagenome_queries.fasta  — ~500 proteins simulating a soil metagenome
  tests/fixtures/diverse_reference.fasta   — ~3,000 proteins from diverse organisms
  tests/fixtures/ground_truth.json         — accession → organism/phylum mapping

Uses the UniProt REST API. Handles pagination, rate limiting, and
organisms with fewer sequences than requested.

Usage:
    python scripts/download_test_fixtures.py
"""

from __future__ import annotations

import json
import random
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"

UNIPROT_BASE = "https://rest.uniprot.org/uniprotkb/stream"

# --- Organism definitions ---

# Query set: soil-like metagenomic community
QUERY_ORGANISMS = [
    ("Bacillus subtilis 168", 224308, 80, "Bacillota"),
    ("Streptomyces coelicolor A3(2)", 100226, 80, "Actinomycetota"),
    ("Pseudomonas putida KT2440", 160488, 80, "Pseudomonadota"),
    ("Methanosarcina acetivorans C2A", 188937, 50, "Euryarchaeota"),
    ("Saccharomyces cerevisiae S288C", 559292, 50, "Ascomycota"),
    ("Nostoc sp. PCC 7120", 103690, 50, "Cyanobacteriota"),
    ("Thermus thermophilus HB8", 300852, 40, "Deinococcota"),
    ("Sulfolobus solfataricus P2", 273057, 40, "Thermoproteota"),
    ("Rhodopirellula baltica SH1", 243090, 30, "Planctomycetota"),
]

# Reference set: broad taxonomic diversity
REFERENCE_ORGANISMS = [
    ("Escherichia coli K-12", 83333, 300),
    ("Bacillus subtilis 168", 224308, 300),
    ("Mycobacterium tuberculosis H37Rv", 83332, 200),
    ("Caulobacter vibrioides CB15", 190650, 150),
    ("Synechocystis sp. PCC 6803", 1111708, 200),
    ("Halobacterium salinarum NRC-1", 64091, 150),
    ("Saccharomyces cerevisiae S288C", 559292, 300),
    ("Arabidopsis thaliana", 3702, 300),
    ("Homo sapiens", 9606, 300),
    ("Deinococcus radiodurans R1", 243230, 100),
    ("Aquifex aeolicus VF5", 224324, 100),
    ("Clostridium acetobutylicum ATCC 824", 272562, 150),
    ("Campylobacter jejuni NCTC 11168", 192222, 100),
]

# Pathogen reference set: clinically relevant organisms
PATHOGEN_ORGANISMS = [
    ("Clostridioides difficile 630", 272563, 100),
    ("Salmonella enterica Typhimurium", 99287, 100),
    ("Staphylococcus aureus NCTC 8325", 93061, 100),
    ("Klebsiella pneumoniae HS11286", 272620, 100),
]


def fetch_uniprot_fasta(
    taxon_id: int,
    max_sequences: int,
    *,
    max_retries: int = 3,
) -> str:
    """Download reviewed Swiss-Prot FASTA for a given organism.

    Args:
        taxon_id: NCBI taxonomy ID.
        max_sequences: Maximum number of sequences to return.
        max_retries: Number of retry attempts on failure.

    Returns:
        FASTA-formatted string.
    """
    url = (
        f"{UNIPROT_BASE}?format=fasta"
        f"&query=(reviewed:true)+AND+(organism_id:{taxon_id})"
    )

    for attempt in range(max_retries):
        try:
            resp = urllib.request.urlopen(url, timeout=60)  # noqa: S310
            data = resp.read().decode("utf-8")
            return data
        except (urllib.error.URLError, TimeoutError) as exc:
            wait = 2 ** (attempt + 1)
            print(
                f"  Retry {attempt + 1}/{max_retries} for taxon {taxon_id}: "
                f"{exc} (waiting {wait}s)"
            )
            time.sleep(wait)

    print(f"  ERROR: Failed to download taxon {taxon_id} after {max_retries} retries")
    return ""


def parse_fasta_entries(fasta_text: str) -> list[tuple[str, str]]:
    """Parse FASTA text into list of (header, sequence) tuples."""
    entries: list[tuple[str, str]] = []
    current_header = ""
    current_seq: list[str] = []

    for line in fasta_text.splitlines():
        if line.startswith(">"):
            if current_header:
                entries.append((current_header, "".join(current_seq)))
            current_header = line
            current_seq = []
        elif line.strip():
            current_seq.append(line.strip())

    if current_header:
        entries.append((current_header, "".join(current_seq)))

    return entries


def extract_accession(header: str) -> str:
    """Extract UniProt accession from a Swiss-Prot header.

    Example: '>sp|P12345|PROT_ECOLI ...' → 'P12345'
    """
    match = re.match(r">sp\|([A-Z0-9]+)\|", header)
    if match:
        return match.group(1)
    # Fallback: first word after >
    return header.split()[0].lstrip(">").split("|")[0]


def extract_protein_name(header: str) -> str:
    """Extract protein name from Swiss-Prot header.

    Example: '>sp|P12345|GLNA_ECOLI Glutamine synthetase OS=...'
             → 'Glutamine synthetase'
    """
    # Text between the ID and OS=
    match = re.search(r"\|[A-Z0-9_]+ (.+?) OS=", header)
    if match:
        return match.group(1)
    return "Unknown protein"


def write_fasta(
    entries: list[tuple[str, str]],
    output_path: Path,
) -> None:
    """Write entries as FASTA with 80-char line wrapping."""
    with output_path.open("w") as f:
        for header, sequence in entries:
            f.write(f"{header}\n")
            for i in range(0, len(sequence), 80):
                f.write(sequence[i : i + 80] + "\n")


def download_query_set() -> None:
    """Download and process the metagenomic query set."""
    print("Downloading query set (simulated soil metagenome)...")

    all_entries: list[tuple[str, str]] = []
    ground_truth: dict[str, dict[str, str]] = {}

    for organism, taxon_id, count, phylum in QUERY_ORGANISMS:
        print(f"  {organism} (taxon {taxon_id}, want {count})...")
        fasta = fetch_uniprot_fasta(taxon_id, count)
        entries = parse_fasta_entries(fasta)

        if len(entries) < count:
            print(f"    Warning: only {len(entries)} available (wanted {count})")

        entries = entries[:count]

        for header, sequence in entries:
            accession = extract_accession(header)
            protein_name = extract_protein_name(header)

            # Record ground truth before stripping organism info
            ground_truth[accession] = {
                "organism": organism,
                "taxon_id": str(taxon_id),
                "phylum": phylum,
            }

            # Create anonymised header (strip organism info)
            anon_header = f">{accession} {protein_name}"
            all_entries.append((anon_header, sequence))

    # Shuffle deterministically
    rng = random.Random(42)
    rng.shuffle(all_entries)

    output = FIXTURES_DIR / "metagenome_queries.fasta"
    write_fasta(all_entries, output)

    gt_path = FIXTURES_DIR / "ground_truth.json"
    gt_path.write_text(json.dumps(ground_truth, indent=2))

    print(f"  Wrote {len(all_entries)} queries to {output}")
    print(f"  Wrote ground truth for {len(ground_truth)} accessions to {gt_path}")


def download_reference_set() -> None:
    """Download the diverse reference set."""
    print("Downloading reference set (taxonomically diverse)...")

    all_entries: list[tuple[str, str]] = []

    for organism, taxon_id, count in REFERENCE_ORGANISMS:
        print(f"  {organism} (taxon {taxon_id}, want {count})...")
        fasta = fetch_uniprot_fasta(taxon_id, count)
        entries = parse_fasta_entries(fasta)

        if len(entries) < count:
            print(f"    Warning: only {len(entries)} available (wanted {count})")

        entries = entries[:count]
        all_entries.extend(entries)

    output = FIXTURES_DIR / "diverse_reference.fasta"
    write_fasta(all_entries, output)

    print(f"  Wrote {len(all_entries)} reference sequences to {output}")


def download_pathogen_reference() -> None:
    """Download pathogen-relevant reference sequences (separate file)."""
    print("Downloading pathogen reference set...")

    output = FIXTURES_DIR / "pathogen_reference.fasta"
    if output.exists():
        print(f"  Skipping (already exists: {output})")
        return

    all_entries: list[tuple[str, str]] = []

    for organism, taxon_id, count in PATHOGEN_ORGANISMS:
        print(f"  {organism} (taxon {taxon_id}, want {count})...")
        fasta = fetch_uniprot_fasta(taxon_id, count)
        entries = parse_fasta_entries(fasta)

        if len(entries) < count:
            print(f"    Warning: only {len(entries)} available (wanted {count})")

        entries = entries[:count]
        all_entries.extend(entries)

    write_fasta(all_entries, output)
    print(f"  Wrote {len(all_entries)} pathogen reference sequences to {output}")


def main() -> None:
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)

    # Only download what's missing
    if not (FIXTURES_DIR / "metagenome_queries.fasta").exists():
        download_query_set()
        print()
    else:
        print("Query set already exists, skipping.")

    if not (FIXTURES_DIR / "diverse_reference.fasta").exists():
        download_reference_set()
        print()
    else:
        print("Diverse reference set already exists, skipping.")

    download_pathogen_reference()
    print()
    print("Done. Fixtures saved to tests/fixtures/")


if __name__ == "__main__":
    main()
