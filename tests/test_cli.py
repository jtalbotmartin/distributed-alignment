"""Tests for the CLI using typer.testing.CliRunner.

Unit tests exercise argument parsing, error handling, and output formatting.
Tests that require DIAMOND are marked @pytest.mark.integration.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from typer.testing import CliRunner

from distributed_alignment.cli import app

if TYPE_CHECKING:
    from pathlib import Path

runner = CliRunner()


class TestHelpOutput:
    """Tests that --help works for all subcommands."""

    def test_main_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "ingest" in result.output
        assert "run" in result.output
        assert "status" in result.output
        assert "explore" in result.output

    def test_ingest_help(self) -> None:
        result = runner.invoke(app, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "--queries" in result.output
        assert "--reference" in result.output
        assert "--output-dir" in result.output
        assert "--chunk-size" in result.output

    def test_run_help(self) -> None:
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "--work-dir" in result.output
        assert "--workers" in result.output
        assert "--sensitivity" in result.output
        assert "--top-n" in result.output

    def test_status_help(self) -> None:
        result = runner.invoke(app, ["status", "--help"])
        assert result.exit_code == 0
        assert "--work-dir" in result.output


class TestExplore:
    """Tests for the explore stub command."""

    def test_explore_exits_cleanly(self) -> None:
        result = runner.invoke(app, ["explore"])
        assert result.exit_code == 0

    def test_explore_prints_not_implemented(self) -> None:
        result = runner.invoke(app, ["explore"])
        assert "not yet implemented" in result.output.lower()


class TestIngest:
    """Tests for the ingest subcommand."""

    def test_ingest_missing_queries_file(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(tmp_path / "nonexistent.fasta"),
                "--reference",
                str(tmp_path / "also_nonexistent.fasta"),
            ],
        )
        assert result.exit_code != 0

    def test_ingest_produces_manifests(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        """Ingest with a real FASTA file produces manifests and chunks."""
        output_dir = tmp_path / "work"

        result = runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(sample_fasta),
                "--reference",
                str(sample_fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

        assert result.exit_code == 0, result.output
        assert (output_dir / "query_manifest.json").exists()
        assert (output_dir / "ref_manifest.json").exists()

        # Verify manifest is valid JSON with expected fields
        q_manifest = json.loads(
            (output_dir / "query_manifest.json").read_text()
        )
        assert q_manifest["total_sequences"] == 3
        assert q_manifest["chunking_strategy"] == "deterministic_hash"

    def test_ingest_prints_summary(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        output_dir = tmp_path / "work"

        result = runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(sample_fasta),
                "--reference",
                str(sample_fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

        assert "Ingestion complete" in result.output
        assert "3 sequences" in result.output

    def test_ingest_creates_chunk_parquet_files(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        output_dir = tmp_path / "work"

        runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(sample_fasta),
                "--reference",
                str(sample_fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

        q_chunks = list(
            (output_dir / "chunks" / "queries").glob("chunk_*.parquet")
        )
        r_chunks = list(
            (output_dir / "chunks" / "references").glob("chunk_*.parquet")
        )
        assert len(q_chunks) >= 1
        assert len(r_chunks) >= 1


class TestStatus:
    """Tests for the status subcommand."""

    def test_status_no_data(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["status", "--work-dir", str(tmp_path)]
        )
        assert result.exit_code == 1
        assert "No pipeline data found" in result.output

    def test_status_after_ingest(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        output_dir = tmp_path / "work"

        # Run ingest first
        runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(sample_fasta),
                "--reference",
                str(sample_fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

        # Check status
        result = runner.invoke(
            app, ["status", "--work-dir", str(output_dir)]
        )
        assert result.exit_code == 0
        assert "3 sequences" in result.output


class TestRunValidation:
    """Tests for run command validation (no DIAMOND needed)."""

    def test_run_no_manifests(self, tmp_path: Path) -> None:
        result = runner.invoke(
            app, ["run", "--work-dir", str(tmp_path)]
        )
        assert result.exit_code == 1
        assert "manifests not found" in result.output

    def test_run_accepts_multiple_workers(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        """--workers N > 1 is accepted (may fail on DIAMOND, that's ok)."""
        output_dir = tmp_path / "work"

        runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(sample_fasta),
                "--reference",
                str(sample_fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

        # Run with --workers 2 — should attempt multi-worker
        result = runner.invoke(
            app,
            ["run", "--work-dir", str(output_dir), "--workers", "2"],
        )
        # Might fail on DIAMOND — but should not fail on arg parsing
        assert "manifests not found" not in result.output
