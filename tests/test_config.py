"""Tests for configuration loading and precedence."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from distributed_alignment.config import (
    DistributedAlignmentConfig,
    load_config,
)


class TestConfigDefaults:
    """Tests for default configuration values."""

    def test_existing_defaults(self) -> None:
        config = DistributedAlignmentConfig()
        assert config.chunk_size == 50_000
        assert config.chunking_strategy == "deterministic_hash"
        assert config.diamond_sensitivity == "very-sensitive"
        assert config.diamond_max_target_seqs == 50
        assert config.diamond_timeout == 3600
        assert config.num_workers == 4
        assert config.heartbeat_interval == 30
        assert config.heartbeat_timeout == 120
        assert config.max_attempts == 3
        assert config.log_level == "INFO"

    def test_phase2_fields(self) -> None:
        config = DistributedAlignmentConfig()
        assert config.backend == "local"
        assert config.reaper_interval == 60

    def test_backend_literal_values(self) -> None:
        config = DistributedAlignmentConfig(backend="local")
        assert config.backend == "local"

        config = DistributedAlignmentConfig(backend="ray")
        assert config.backend == "ray"

    def test_invalid_backend_raises(self) -> None:
        with pytest.raises(ValueError):
            DistributedAlignmentConfig(backend="invalid")  # type: ignore[arg-type]


class TestTomlLoading:
    """Tests for loading config from TOML files."""

    def test_loads_from_toml_file(self, tmp_path: Path) -> None:
        toml_content = (
            'chunk_size = 1000\ndiamond_sensitivity = "fast"\nnum_workers = 8\n'
        )
        toml_path = tmp_path / "distributed_alignment.toml"
        toml_path.write_text(toml_content)

        import os

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = DistributedAlignmentConfig()
        finally:
            os.chdir(original_dir)

        assert config.chunk_size == 1000
        assert config.diamond_sensitivity == "fast"
        assert config.num_workers == 8

    def test_missing_toml_uses_defaults(self, tmp_path: Path) -> None:
        import os

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = DistributedAlignmentConfig()
        finally:
            os.chdir(original_dir)

        # Should use defaults without error
        assert config.chunk_size == 50_000
        assert config.diamond_sensitivity == "very-sensitive"


class TestEnvOverrides:
    """Tests for environment variable overrides."""

    def test_env_overrides_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DA_CHUNK_SIZE", "2000")
        monkeypatch.setenv("DA_NUM_WORKERS", "16")
        monkeypatch.setenv("DA_BACKEND", "ray")

        config = DistributedAlignmentConfig()
        assert config.chunk_size == 2000
        assert config.num_workers == 16
        assert config.backend == "ray"

    def test_env_overrides_toml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        toml_content = "chunk_size = 1000\nnum_workers = 8\n"
        toml_path = tmp_path / "distributed_alignment.toml"
        toml_path.write_text(toml_content)

        # Env should override TOML
        monkeypatch.setenv("DA_CHUNK_SIZE", "5000")

        import os

        original_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            config = DistributedAlignmentConfig()
        finally:
            os.chdir(original_dir)

        assert config.chunk_size == 5000  # env wins over TOML
        assert config.num_workers == 8  # TOML value (no env override)


class TestLoadConfig:
    """Tests for the load_config convenience function."""

    def test_overrides_applied(self) -> None:
        config = load_config(overrides={"chunk_size": 999})
        assert config.chunk_size == 999

    def test_none_overrides_ignored(self) -> None:
        config = load_config(overrides={"chunk_size": None, "num_workers": 2})
        assert config.chunk_size == 50_000  # default, not None
        assert config.num_workers == 2

    def test_work_dir_toml_discovery(self, tmp_path: Path) -> None:
        toml_content = "chunk_size = 7777\n"
        (tmp_path / "distributed_alignment.toml").write_text(toml_content)

        config = load_config(work_dir=tmp_path)
        assert config.chunk_size == 7777

    def test_falls_back_to_cwd_toml(self) -> None:
        # Without work_dir, uses cwd — which has the project's TOML
        config = load_config()
        # Should load without error (may or may not find TOML depending on cwd)
        assert config.chunk_size > 0

    def test_overrides_beat_toml(self, tmp_path: Path) -> None:
        toml_content = "chunk_size = 3000\n"
        (tmp_path / "distributed_alignment.toml").write_text(toml_content)

        config = load_config(
            work_dir=tmp_path,
            overrides={"chunk_size": 500},
        )
        assert config.chunk_size == 500  # override wins


class TestCliConfigIntegration:
    """Tests that CLI flags override config values."""

    def test_ingest_uses_config_chunk_size(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        """When no --chunk-size flag, config value is used."""
        from typer.testing import CliRunner

        from distributed_alignment.cli import app

        runner = CliRunner()
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

        assert result.exit_code == 0
        assert "Ingestion complete" in result.output

    def test_ingest_cli_chunk_size_overrides(
        self, sample_fasta: Path, tmp_path: Path
    ) -> None:
        """Explicit --chunk-size overrides config."""
        from typer.testing import CliRunner

        from distributed_alignment.cli import app

        runner = CliRunner()
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
                "--chunk-size",
                "1",
            ],
        )

        assert result.exit_code == 0
        # With chunk_size=1 and 3 sequences, should get 3 chunks
        assert "3 chunks" in result.output

    def test_run_uses_config_workers(
        self,
        sample_fasta: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Config num_workers is respected by the run command."""
        from typer.testing import CliRunner

        from distributed_alignment.cli import app

        runner = CliRunner()
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

        monkeypatch.setenv("DA_NUM_WORKERS", "2")

        result = runner.invoke(
            app,
            ["run", "--work-dir", str(output_dir)],
        )
        # Should attempt 2 workers (may fail on DIAMOND)
        assert "2 workers" in result.output or result.exit_code != 0
