"""Shared test fixtures for distributed-alignment."""

from pathlib import Path

import pytest


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
