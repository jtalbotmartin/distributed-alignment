"""Application configuration using Pydantic Settings.

Layered precedence: defaults → config file → env vars (DA_ prefix) → CLI args.
"""

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

    # Paths
    work_dir: Path = Path("./work")
    results_dir: Path = Path("./results")
    features_dir: Path = Path("./features")
    catalogue_path: Path = Path("./catalogue.duckdb")

    # Observability
    log_level: str = "INFO"
    metrics_port: int = 9090
    enable_cost_tracking: bool = True
    cost_per_cpu_hour: float = 0.0464
