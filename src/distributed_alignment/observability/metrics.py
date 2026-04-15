"""Prometheus metrics for the distributed-alignment pipeline.

Defines all metrics as module-level objects and provides helper
functions for recording pipeline events. Uses prometheus_client's
in-process registry.
"""

from __future__ import annotations

import structlog
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

logger = structlog.get_logger(component="metrics")

# --- Metrics definitions ---

da_packages_total = Gauge(
    "da_packages_total",
    "Work packages by state",
    ["state"],
)

da_package_duration_seconds = Histogram(
    "da_package_duration_seconds",
    "Time to process one work package",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

da_sequences_processed = Counter(
    "da_sequences_processed",
    "Total sequences aligned",
)

da_hits_found = Counter(
    "da_hits_found",
    "Total alignment hits found",
)

da_worker_count = Gauge(
    "da_worker_count",
    "Number of active workers",
)

da_errors = Counter(
    "da_errors",
    "Errors by type",
    ["error_type"],
)

da_diamond_exit_code = Counter(
    "da_diamond_exit_code",
    "DIAMOND exit codes",
    ["exit_code"],
)

# Convenience aliases matching TDD naming convention
da_sequences_processed_total = da_sequences_processed
da_hits_found_total = da_hits_found
da_errors_total = da_errors


# --- Helper functions ---


def start_metrics_server(port: int = 9090) -> bool:
    """Start the Prometheus metrics HTTP server.

    Catches ``OSError`` if the port is already in use (e.g. another
    worker on the same machine).

    Args:
        port: TCP port for the metrics endpoint.

    Returns:
        True if the server started, False if the port was busy.
    """
    try:
        start_http_server(port)
        logger.info("metrics_server_started", port=port)
        return True
    except OSError:
        logger.warning(
            "metrics_server_port_busy",
            port=port,
        )
        return False


def record_package_completed(
    duration_seconds: float,
    num_sequences: int,
    num_hits: int,
) -> None:
    """Record a successfully completed work package.

    Args:
        duration_seconds: Wall-clock time to process the package.
        num_sequences: Number of query sequences in the chunk.
        num_hits: Number of alignment hits produced.
    """
    da_package_duration_seconds.observe(duration_seconds)
    da_sequences_processed.inc(num_sequences)
    da_hits_found.inc(num_hits)


def record_package_failed(error_type: str) -> None:
    """Record a failed work package.

    Args:
        error_type: Category of the error (e.g. "oom", "timeout",
            "diamond_error", "write_error").
    """
    da_errors.labels(error_type=error_type).inc()


def record_diamond_result(exit_code: int) -> None:
    """Record a DIAMOND execution result.

    Args:
        exit_code: DIAMOND's process exit code.
    """
    da_diamond_exit_code.labels(exit_code=str(exit_code)).inc()


def update_package_states(status: dict[str, int]) -> None:
    """Update the package state gauge from work stack status.

    Args:
        status: Dict mapping state name to count, e.g.
            ``{"PENDING": 5, "RUNNING": 2, "COMPLETED": 10, ...}``.
    """
    for state, count in status.items():
        da_packages_total.labels(state=state).set(count)
