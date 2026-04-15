"""Observability: structured logging and Prometheus metrics."""

from distributed_alignment.observability.logging import configure_logging
from distributed_alignment.observability.metrics import (
    dec_worker,
    get_metrics,
    inc_worker,
    record_diamond_result,
    record_package_completed,
    record_package_failed,
    reset_metrics,
    start_metrics_server,
    update_package_states,
)

__all__ = [
    "configure_logging",
    "dec_worker",
    "get_metrics",
    "inc_worker",
    "record_diamond_result",
    "record_package_completed",
    "record_package_failed",
    "reset_metrics",
    "start_metrics_server",
    "update_package_states",
]
