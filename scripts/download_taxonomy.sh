#!/usr/bin/env bash
# Download NCBI taxonomy data for the enrichment pipeline.
#
# Downloads:
#   1. taxdump.tar.gz  — taxonomy tree (names.dmp, nodes.dmp)
#   2. prot.accession2taxid.gz — protein accession → taxon ID (~2.5 GB compressed)
#
# Saves to data/taxonomy/ (gitignored).  Idempotent — skips files that
# already exist.
#
# Usage:
#     bash scripts/download_taxonomy.sh

set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/taxonomy"
mkdir -p "$DATA_DIR"

NCBI_FTP="https://ftp.ncbi.nlm.nih.gov/pub/taxonomy"

# ── Taxonomy tree ──────────────────────────────────────────────────────────

if [ -f "$DATA_DIR/nodes.dmp" ] && [ -f "$DATA_DIR/names.dmp" ]; then
    echo "nodes.dmp and names.dmp already exist — skipping taxdump download."
else
    echo "Downloading taxdump.tar.gz..."
    curl -fSL -o "$DATA_DIR/taxdump.tar.gz" "$NCBI_FTP/taxdump.tar.gz"
    echo "Extracting nodes.dmp and names.dmp..."
    tar xzf "$DATA_DIR/taxdump.tar.gz" -C "$DATA_DIR" names.dmp nodes.dmp
    rm -f "$DATA_DIR/taxdump.tar.gz"
    echo "Done."
fi

# ── Protein accession → taxon mapping ─────────────────────────────────────

if [ -f "$DATA_DIR/prot.accession2taxid.gz" ]; then
    echo "prot.accession2taxid.gz already exists — skipping."
else
    echo "Downloading prot.accession2taxid.gz (~2.5 GB compressed)..."
    curl -fSL -o "$DATA_DIR/prot.accession2taxid.gz" \
        "$NCBI_FTP/accession2taxid/prot.accession2taxid.gz"
    echo "Done."
fi

echo ""
echo "Taxonomy data saved to $DATA_DIR"
echo "  nodes.dmp                — taxonomy tree nodes"
echo "  names.dmp                — taxon names"
echo "  prot.accession2taxid.gz  — protein accession → taxon ID"
