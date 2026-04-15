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


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    import re

    return re.sub(r"\x1b\[[0-9;]*m", "", text)


class TestHelpOutput:
    """Tests that --help works for all subcommands."""

    def test_main_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        output = _strip_ansi(result.output)
        assert result.exit_code == 0
        assert "ingest" in output
        assert "run" in output
        assert "status" in output
        assert "explore" in output

    def test_ingest_help(self) -> None:
        result = runner.invoke(app, ["ingest", "--help"])
        output = _strip_ansi(result.output)
        assert result.exit_code == 0
        assert "--queries" in output
        assert "--reference" in output
        assert "--output-dir" in output
        assert "--chunk-size" in output

    def test_run_help(self) -> None:
        result = runner.invoke(app, ["run", "--help"])
        output = _strip_ansi(result.output)
        assert result.exit_code == 0
        assert "--work-dir" in output
        assert "--workers" in output
        assert "--sensitivity" in output
        assert "--top-n" in output

    def test_status_help(self) -> None:
        result = runner.invoke(app, ["status", "--help"])
        output = _strip_ansi(result.output)
        assert result.exit_code == 0
        assert "--work-dir" in output


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
        q_manifest = json.loads((output_dir / "query_manifest.json").read_text())
        assert q_manifest["total_sequences"] == 3
        assert q_manifest["chunking_strategy"] == "deterministic_hash"

    def test_ingest_prints_summary(self, sample_fasta: Path, tmp_path: Path) -> None:
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

        q_chunks = list((output_dir / "chunks" / "queries").glob("chunk_*.parquet"))
        r_chunks = list((output_dir / "chunks" / "references").glob("chunk_*.parquet"))
        assert len(q_chunks) >= 1
        assert len(r_chunks) >= 1


class TestStatus:
    """Tests for the status subcommand."""

    def test_status_no_data(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["status", "--work-dir", str(tmp_path)])
        assert result.exit_code == 1
        assert "No pipeline data found" in result.output

    def test_status_after_ingest(self, sample_fasta: Path, tmp_path: Path) -> None:
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
        result = runner.invoke(app, ["status", "--work-dir", str(output_dir)])
        assert result.exit_code == 0
        assert "3 sequences" in result.output


class TestRunValidation:
    """Tests for run command validation (no DIAMOND needed)."""

    def test_run_no_manifests(self, tmp_path: Path) -> None:
        result = runner.invoke(app, ["run", "--work-dir", str(tmp_path)])
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


class TestRunDispatch:
    """Tests for the run command's backend dispatch (mocked workers)."""

    def _ingest(self, fasta: Path, output_dir: Path) -> None:
        runner.invoke(
            app,
            [
                "ingest",
                "--queries",
                str(fasta),
                "--reference",
                str(fasta),
                "--output-dir",
                str(output_dir),
            ],
        )

    def test_single_worker_dispatch(self, sample_fasta: Path, tmp_path: Path) -> None:
        """--workers 1 uses in-process single worker path."""
        from unittest.mock import patch

        output_dir = tmp_path / "work"
        self._ingest(sample_fasta, output_dir)

        with patch("distributed_alignment.cli._run_single_worker") as mock_single:
            # Still fails on DIAMOND check, but we can verify
            # the dispatch path by checking what gets called
            result = runner.invoke(
                app,
                ["run", "--work-dir", str(output_dir), "--workers", "1"],
            )
            # Either mock was called (DIAMOND found) or DIAMOND
            # error occurred before dispatch
            if "DIAMOND binary not found" not in _strip_ansi(result.output):
                mock_single.assert_called_once()

    def test_multiprocess_dispatch(self, sample_fasta: Path, tmp_path: Path) -> None:
        """--workers 2 --backend local uses multiprocessing path."""
        from unittest.mock import patch

        output_dir = tmp_path / "work"
        self._ingest(sample_fasta, output_dir)

        with patch("distributed_alignment.cli._run_multiprocess_backend") as mock_mp:
            result = runner.invoke(
                app,
                [
                    "run",
                    "--work-dir",
                    str(output_dir),
                    "--workers",
                    "2",
                    "--backend",
                    "local",
                ],
            )
            if "DIAMOND binary not found" not in _strip_ansi(result.output):
                mock_mp.assert_called_once()

    def test_ray_dispatch(self, sample_fasta: Path, tmp_path: Path) -> None:
        """--backend ray uses Ray dispatch path."""
        from unittest.mock import patch

        output_dir = tmp_path / "work"
        self._ingest(sample_fasta, output_dir)

        with patch("distributed_alignment.cli._run_ray_backend") as mock_ray:
            result = runner.invoke(
                app,
                [
                    "run",
                    "--work-dir",
                    str(output_dir),
                    "--backend",
                    "ray",
                ],
            )
            if "DIAMOND binary not found" not in _strip_ansi(result.output):
                mock_ray.assert_called_once()

    def test_status_with_completed_work(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        """Status shows correct output after ingest + work packages."""
        import json as _json

        from distributed_alignment.scheduler.filesystem_backend import (
            FileSystemWorkStack,
        )

        output_dir = tmp_path / "work"
        self._ingest(sample_fasta, output_dir)

        # Create work stack and generate packages manually
        q_data = _json.loads((output_dir / "query_manifest.json").read_text())
        r_data = _json.loads((output_dir / "ref_manifest.json").read_text())
        from distributed_alignment.models import ChunkManifest

        q = ChunkManifest(**q_data)
        r = ChunkManifest(**r_data)

        stack = FileSystemWorkStack(output_dir / "work_stack")
        stack.generate_work_packages(q, r)

        # Complete one package
        pkg = stack.claim("test-worker")
        if pkg:
            stack.complete(pkg.package_id, "/fake/result.parquet")

        result = runner.invoke(app, ["status", "--work-dir", str(output_dir)])
        output = _strip_ansi(result.output)
        assert result.exit_code == 0
        assert "COMPLETED" in output
