"""Smoke tests verifying project scaffolding is correctly set up."""

from pathlib import Path

import pytest

from distributed_alignment import __version__
from distributed_alignment.config import DistributedAlignmentConfig
from distributed_alignment.models import (
    ChunkEntry,
    ChunkManifest,
    FeatureRow,
    MergedHit,
    ProteinSequence,
    WorkPackage,
    WorkPackageState,
)


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_config_defaults() -> None:
    config = DistributedAlignmentConfig()
    assert config.chunk_size == 50_000
    assert config.chunking_strategy == "deterministic_hash"
    assert config.diamond_sensitivity == "very-sensitive"
    assert config.num_workers == 4
    assert config.max_attempts == 3
    assert config.work_dir == Path("./work")
    assert config.log_level == "INFO"


def test_config_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DA_CHUNK_SIZE", "1000")
    monkeypatch.setenv("DA_NUM_WORKERS", "8")
    config = DistributedAlignmentConfig()
    assert config.chunk_size == 1000
    assert config.num_workers == 8


def test_protein_sequence_valid() -> None:
    seq = ProteinSequence(
        id="P12345",
        description="Test protein",
        sequence="MKWVTFISLLFLFSSAYS",
        length=18,
    )
    assert seq.sequence == "MKWVTFISLLFLFSSAYS"
    assert seq.length == 18


def test_protein_sequence_normalises_case() -> None:
    seq = ProteinSequence(
        id="P12345",
        description="Test protein",
        sequence="mkwvtfis",
        length=8,
    )
    assert seq.sequence == "MKWVTFIS"


def test_protein_sequence_rejects_invalid_chars() -> None:
    import pytest

    with pytest.raises(ValueError, match="Invalid amino acid characters"):
        ProteinSequence(
            id="P12345",
            description="Test protein",
            sequence="MKWVT123FIS",
            length=11,
        )


def test_protein_sequence_rejects_zero_length() -> None:
    import pytest

    with pytest.raises(ValueError, match="Sequence length must be positive"):
        ProteinSequence(
            id="P12345",
            description="Test protein",
            sequence="MKWVTFIS",
            length=0,
        )


def test_work_package_defaults() -> None:
    wp = WorkPackage(
        package_id="wp_q000_r000",
        query_chunk_id="q000",
        ref_chunk_id="r000",
    )
    assert wp.state == WorkPackageState.PENDING
    assert wp.attempt == 0
    assert wp.claimed_by is None
    assert wp.error_history == []


def test_work_package_state_values() -> None:
    assert WorkPackageState.PENDING == "PENDING"
    assert WorkPackageState.RUNNING == "RUNNING"
    assert WorkPackageState.COMPLETED == "COMPLETED"
    assert WorkPackageState.FAILED == "FAILED"
    assert WorkPackageState.POISONED == "POISONED"


def test_merged_hit() -> None:
    hit = MergedHit(
        query_id="P12345",
        subject_id="Q67890",
        percent_identity=85.5,
        alignment_length=150,
        mismatches=22,
        gap_opens=1,
        query_start=1,
        query_end=150,
        subject_start=10,
        subject_end=160,
        evalue=1e-45,
        bitscore=250.0,
        global_rank=1,
        query_chunk_id="q000",
        ref_chunk_id="r000",
    )
    assert hit.global_rank == 1


def test_chunk_manifest() -> None:
    from datetime import UTC, datetime

    manifest = ChunkManifest(
        run_id="run_20260404_143022",
        input_files=["proteins.fasta"],
        total_sequences=100,
        num_chunks=2,
        chunk_size_target=50,
        chunks=[
            ChunkEntry(
                chunk_id="q000",
                num_sequences=48,
                parquet_path="chunks/queries/chunk_q000.parquet",
                content_checksum="sha256:abc123",
            ),
            ChunkEntry(
                chunk_id="q001",
                num_sequences=52,
                parquet_path="chunks/queries/chunk_q001.parquet",
                content_checksum="sha256:def456",
            ),
        ],
        created_at=datetime.now(tz=UTC),
        chunking_strategy="deterministic_hash",
    )
    assert manifest.total_sequences == 100
    assert len(manifest.chunks) == 2


def test_feature_row() -> None:
    from datetime import UTC, datetime

    row = FeatureRow(
        sequence_id="P12345",
        hit_count=25,
        mean_percent_identity=72.3,
        max_percent_identity=95.1,
        mean_evalue_log10=-15.2,
        mean_alignment_length=120.5,
        std_alignment_length=30.2,
        best_hit_query_coverage=0.85,
        taxonomic_entropy=2.1,
        num_phyla=5,
        num_kingdoms=2,
        feature_version="v1",
        run_id="run_20260404_143022",
        created_at=datetime.now(tz=UTC),
    )
    assert row.hit_count == 25
    assert row.num_phyla == 5


def test_work_dir_fixture(work_dir: Path) -> None:
    assert (work_dir / "pending").is_dir()
    assert (work_dir / "running").is_dir()
    assert (work_dir / "completed").is_dir()
    assert (work_dir / "poisoned").is_dir()
    assert (work_dir / "results").is_dir()
    assert (work_dir / "chunks" / "queries").is_dir()
    assert (work_dir / "chunks" / "references").is_dir()


def test_sample_fasta_fixture(sample_fasta: Path) -> None:
    assert sample_fasta.exists()
    content = sample_fasta.read_text()
    assert content.startswith(">sp|P12345|PROT1")
    assert content.count(">") == 3
