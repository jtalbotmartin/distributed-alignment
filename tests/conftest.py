"""Shared test fixtures for distributed-alignment."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Swiss-Prot fixture paths (committed to repo)
SWISSPROT_QUERIES = FIXTURES_DIR / "swissprot_queries.fasta"
SWISSPROT_REFERENCE = FIXTURES_DIR / "swissprot_reference.fasta"


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    """Create a temporary working directory with the expected subdirectory structure."""
    subdirs = [
        "chunks/queries",
        "chunks/references",
        "pending",
        "running",
        "completed",
        "poisoned",
        "results",
    ]
    for subdir in subdirs:
        (tmp_path / subdir).mkdir(parents=True)
    return tmp_path


@pytest.fixture
def sample_fasta(tmp_path: Path) -> Path:
    """Create a small valid FASTA file for testing."""
    fasta_content = (
        ">sp|P12345|PROT1 Test protein 1\n"
        "MKWVTFISLLFLFSSAYS\n"
        "RGVFRRDAHKSEVAHRFK\n"
        ">sp|P67890|PROT2 Test protein 2\n"
        "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTK\n"
        ">sp|Q11111|PROT3 Test protein 3\n"
        "MNIFEMLRIDEGLRLKIYKDTEGYYTIGIGHLLTKSPSLNAAKSELDKAIGRN\n"
    )
    fasta_path = tmp_path / "test.fasta"
    fasta_path.write_text(fasta_content)
    return fasta_path


@pytest.fixture
def sample_sequences() -> list[dict[str, str | int]]:
    """Return a list of raw sequence data for testing models."""
    return [
        {
            "id": "P12345",
            "description": "sp|P12345|PROT1 Test protein 1",
            "sequence": "MKWVTFISLLFLFSSAYSRGVFRRDAHKSEVAHRFK",
            "length": 36,
        },
        {
            "id": "P67890",
            "description": "sp|P67890|PROT2 Test protein 2",
            "sequence": "MVLSPADKTNVKAAWGKVGAHAGEYGAEALERMFLSFPTTK",
            "length": 41,
        },
        {
            "id": "Q11111",
            "description": "sp|Q11111|PROT3 Test protein 3",
            "sequence": "MNIFEMLRIDEGLRLKIYKDTEGYYTIGIGHLLTKSPSLNAAKSELDKAIGRN",
            "length": 53,
        },
    ]


@pytest.fixture
def integration_test_data(
    tmp_path: Path,
) -> tuple[Path, Path]:
    """Provide query and reference FASTA files for integration tests.

    Uses committed Swiss-Prot fixtures when available (real protein
    sequences — produces meaningful DIAMOND hits). Falls back to
    synthetic data if fixtures are missing.
    """
    queries = tmp_path / "queries.fasta"
    reference = tmp_path / "reference.fasta"

    if SWISSPROT_QUERIES.exists() and SWISSPROT_REFERENCE.exists():
        shutil.copy(SWISSPROT_QUERIES, queries)
        shutil.copy(SWISSPROT_REFERENCE, reference)
    else:
        from scripts.generate_test_data import generate_fasta

        generate_fasta(
            queries,
            num_sequences=10,
            prefix="query",
            min_length=50,
            max_length=100,
            seed=42,
        )
        generate_fasta(
            reference,
            num_sequences=50,
            prefix="ref",
            min_length=50,
            max_length=100,
            seed=123,
        )

    return queries, reference
