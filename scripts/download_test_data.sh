#!/usr/bin/env bash
# Download Swiss-Prot sequences for integration testing.
#
# Fetches reviewed protein sequences from UniProt:
#   - Queries:   100 human proteins (organism_id:9606)
#   - Reference: 500 E. coli K-12 proteins (organism_id:83333)
#
# Output is saved to tests/fixtures/ and should be committed so
# tests don't depend on network access.
#
# Usage:
#   bash scripts/download_test_data.sh
#
# Requires: curl, python3

set -euo pipefail

FIXTURES_DIR="tests/fixtures"
mkdir -p "$FIXTURES_DIR"

UNIPROT_BASE="https://rest.uniprot.org/uniprotkb/stream"

echo "Downloading Swiss-Prot test data..."

# Download and trim reference set (500 E. coli K-12 proteins)
echo "  Fetching E. coli K-12 proteins (reference)..."
curl -s "${UNIPROT_BASE}?query=(reviewed:true)+AND+(organism_id:83333)&format=fasta" \
    -o /tmp/swissprot_ecoli_full.fasta

python3 -c "
count = 0
lines = []
with open('/tmp/swissprot_ecoli_full.fasta') as f:
    for line in f:
        if line.startswith('>'):
            if count >= 500:
                break
            count += 1
        lines.append(line)
with open('${FIXTURES_DIR}/swissprot_reference.fasta', 'w') as f:
    f.writelines(lines)
print(f'  Wrote {count} E. coli reference sequences')
"

# Download and trim query set (100 human proteins)
echo "  Fetching human proteins (queries)..."
curl -s "${UNIPROT_BASE}?query=(reviewed:true)+AND+(organism_id:9606)&format=fasta" \
    -o /tmp/swissprot_human_full.fasta

python3 -c "
count = 0
lines = []
with open('/tmp/swissprot_human_full.fasta') as f:
    for line in f:
        if line.startswith('>'):
            if count >= 100:
                break
            count += 1
        lines.append(line)
with open('${FIXTURES_DIR}/swissprot_queries.fasta', 'w') as f:
    f.writelines(lines)
print(f'  Wrote {count} human query sequences')
"

# Verify
Q_COUNT=$(grep -c '^>' "${FIXTURES_DIR}/swissprot_queries.fasta" || true)
R_COUNT=$(grep -c '^>' "${FIXTURES_DIR}/swissprot_reference.fasta" || true)

echo ""
echo "Saved to ${FIXTURES_DIR}/:"
echo "  swissprot_queries.fasta:   ${Q_COUNT} sequences (human, Swiss-Prot)"
echo "  swissprot_reference.fasta: ${R_COUNT} sequences (E. coli K-12, Swiss-Prot)"

if [ "$Q_COUNT" -lt 10 ] || [ "$R_COUNT" -lt 10 ]; then
    echo ""
    echo "WARNING: Fewer sequences than expected — check network connection."
    echo "Fallback: python scripts/generate_test_data.py"
    exit 1
fi
