"""Application configuration using Pydantic Settings.

Layered precedence: defaults → config file → env vars (DA_ prefix) → CLI args.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


class DistributedAlignmentConfig(BaseSettings):
    """Root configuration for the distributed-alignment pipeline.

    All settings can be overridden via environment variables with the ``DA_`` prefix
    (e.g. ``DA_CHUNK_SIZE=10000``) or via ``distributed_alignment.toml``.
    """

    model_config = SettingsConfigDict(
        env_prefix="DA_",
        env_nested_delimiter="__",
        toml_file="distributed_alignment.toml",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Configure settings source precedence.

        Order (highest to lowest priority):
        init kwargs → env vars → TOML file → defaults.
        """
        return (
            init_settings,
            env_settings,
            TomlConfigSettingsSource(settings_cls),
        )

    # Ingestion
    chunk_size: int = 50_000
    chunking_strategy: Literal["deterministic_hash", "sequential"] = (
        "deterministic_hash"
    )

    # DIAMOND
    diamond_binary: str = "diamond"
    diamond_sensitivity: Literal[
        "fast", "sensitive", "very-sensitive", "ultra-sensitive"
    ] = "very-sensitive"
    diamond_max_target_seqs: int = 50
    diamond_timeout: int = 3600

    # Workers
    num_workers: int = 4
    heartbeat_interval: int = 30
    heartbeat_timeout: int = 120
    max_attempts: int = 3
    backend: Literal["local", "ray"] = "local"

    # Reaper
    reaper_interval: int = 60

    # Paths — all defaults nest under ./work/ so a `run` invocation
    # doesn't leak artifacts into the repo root.  Override any of them
    # via TOML/env/CLI to move outputs elsewhere.
    work_dir: Path = Path("./work")
    results_dir: Path = Path("./work/results")
    features_dir: Path = Path("./work/features")
    catalogue_path: Path = Path("./work/catalogue.duckdb")
    taxonomy_db_path: Path | None = None
    embeddings_path: Path | None = None

    # Observability
    log_level: str = "INFO"
    metrics_port: int = 9090
    enable_cost_tracking: bool = True
    cost_per_cpu_hour: float = 0.0464


def load_config(
    *,
    work_dir: Path | None = None,
    overrides: dict[str, object] | None = None,
) -> DistributedAlignmentConfig:
    """Load configuration with TOML file discovery.

    Searches for ``distributed_alignment.toml`` in the given work
    directory, then falls back to the current working directory.
    Environment variables (``DA_*``) override TOML values.
    Explicit overrides (from CLI flags) take highest priority.

    Args:
        work_dir: Directory to search for the TOML config file.
            Falls back to cwd if not provided or file not found.
        overrides: Dict of field names to values from CLI flags.
            Only non-None values are applied.

    Returns:
        Fully resolved configuration.
    """
    import os

    init_kwargs: dict[str, object] = {}
    if overrides:
        init_kwargs = {k: v for k, v in overrides.items() if v is not None}

    # pydantic-settings TomlConfigSettingsSource reads from cwd.
    # If work_dir has a TOML file, temporarily chdir so it's found.
    toml_dir: Path | None = None
    if work_dir is not None:
        candidate = work_dir / "distributed_alignment.toml"
        if candidate.exists():
            toml_dir = work_dir

    original_dir = os.getcwd()
    try:
        if toml_dir is not None:
            os.chdir(toml_dir)
        config = DistributedAlignmentConfig(**init_kwargs)  # type: ignore[arg-type]
    finally:
        os.chdir(original_dir)

    return config
