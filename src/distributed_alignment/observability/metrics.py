"""Prometheus metrics for the distributed-alignment pipeline.

Provides a dual backend: prometheus_client for local/multiprocessing,
ray.util.metrics for Ray actors. The helper functions auto-detect the
backend and delegate accordingly.
"""

from __future__ import annotations

import structlog
from prometheus_client import Counter as _Counter
from prometheus_client import Gauge as _Gauge
from prometheus_client import Histogram as _Histogram

logger = structlog.get_logger(component="metrics")


# --- Module-level prometheus_client metrics (registered once) ---

_prom_packages_total = _Gauge(
    "da_packages_total", "Work packages by state", ["state"],
)
_prom_package_duration = _Histogram(
    "da_package_duration_seconds",
    "Time to process one work package",
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)
_prom_sequences_processed = _Counter(
    "da_sequences_processed", "Total sequences aligned",
)
_prom_hits_found = _Counter(
    "da_hits_found", "Total alignment hits found",
)
_prom_worker_count = _Gauge(
    "da_worker_count", "Number of active workers",
)
_prom_errors = _Counter(
    "da_errors", "Errors by type", ["error_type"],
)
_prom_diamond_exit_code = _Counter(
    "da_diamond_exit_code", "DIAMOND exit codes", ["exit_code"],
)


class PrometheusMetrics:
    """Metrics backed by prometheus_client (local/multiprocessing).

    References module-level metric objects to avoid duplicate
    registration in prometheus_client's global registry.
    """

    def __init__(self) -> None:
        self.packages_total = _prom_packages_total
        self.package_duration = _prom_package_duration
        self.sequences_processed = _prom_sequences_processed
        self.hits_found = _prom_hits_found
        self.worker_count = _prom_worker_count
        self.errors = _prom_errors
        self.diamond_exit_code = _prom_diamond_exit_code

    def observe_duration(self, seconds: float) -> None:
        self.package_duration.observe(seconds)

    def inc_sequences(self, n: int) -> None:
        self.sequences_processed.inc(n)

    def inc_hits(self, n: int) -> None:
        self.hits_found.inc(n)

    def inc_worker(self) -> None:
        self.worker_count.inc()

    def dec_worker(self) -> None:
        self.worker_count.dec()

    def inc_error(self, error_type: str) -> None:
        self.errors.labels(error_type=error_type).inc()

    def inc_diamond_exit(self, exit_code: int) -> None:
        self.diamond_exit_code.labels(
            exit_code=str(exit_code)
        ).inc()

    def set_package_state(self, state: str, count: int) -> None:
        self.packages_total.labels(state=state).set(count)


class RayMetrics:
    """Metrics backed by ray.util.metrics (Ray actors).

    Ray automatically aggregates these across all actors and exposes
    them via Ray's built-in metrics endpoint.
    """

    def __init__(self) -> None:
        from ray.util.metrics import Counter, Gauge, Histogram

        self.package_duration = Histogram(
            "da_package_duration_seconds",
            description="Time to process one work package",
            boundaries=[1, 5, 10, 30, 60, 120, 300, 600],
        )
        self.sequences_processed = Counter(
            "da_sequences_processed",
            description="Total sequences aligned",
        )
        self.hits_found = Counter(
            "da_hits_found",
            description="Total alignment hits found",
        )
        self.worker_count = Gauge(
            "da_worker_count",
            description="Number of active workers",
        )
        self.errors = Counter(
            "da_errors",
            description="Errors by type",
            tag_keys=("error_type",),
        )
        self.diamond_exit_code = Counter(
            "da_diamond_exit_code",
            description="DIAMOND exit codes",
            tag_keys=("exit_code",),
        )
        self.packages_total = Gauge(
            "da_packages_total",
            description="Work packages by state",
            tag_keys=("state",),
        )

    def observe_duration(self, seconds: float) -> None:
        self.package_duration.observe(seconds)

    def inc_sequences(self, n: int) -> None:
        # ray.util.metrics Counter requires value > 0
        if n > 0:
            self.sequences_processed.inc(n)

    def inc_hits(self, n: int) -> None:
        if n > 0:
            self.hits_found.inc(n)

    def inc_worker(self) -> None:
        # ray.util.metrics Gauge only has .set() — track manually
        self._worker_val = getattr(self, "_worker_val", 0) + 1
        self.worker_count.set(self._worker_val)

    def dec_worker(self) -> None:
        self._worker_val = max(0, getattr(self, "_worker_val", 1) - 1)
        self.worker_count.set(self._worker_val)

    def inc_error(self, error_type: str) -> None:
        self.errors.inc(tags={"error_type": error_type})

    def inc_diamond_exit(self, exit_code: int) -> None:
        self.diamond_exit_code.inc(
            tags={"exit_code": str(exit_code)}
        )

    def set_package_state(self, state: str, count: int) -> None:
        self.packages_total.set(count, tags={"state": state})


# --- Singleton management ---

_metrics_instance: PrometheusMetrics | RayMetrics | None = None


def get_metrics() -> PrometheusMetrics | RayMetrics:
    """Get or create the metrics singleton.

    Auto-detects the backend: uses RayMetrics if Ray is initialised,
    PrometheusMetrics otherwise.
    """
    global _metrics_instance  # noqa: PLW0603
    if _metrics_instance is not None:
        return _metrics_instance

    try:
        import ray

        if ray.is_initialized():
            _metrics_instance = RayMetrics()
            logger.info("metrics_backend", backend="ray")
            return _metrics_instance
    except ImportError:
        pass

    _metrics_instance = PrometheusMetrics()
    logger.info("metrics_backend", backend="prometheus")
    return _metrics_instance


def reset_metrics() -> None:
    """Clear the metrics singleton (for testing)."""
    global _metrics_instance  # noqa: PLW0603
    _metrics_instance = None


# --- Public helper functions ---


def start_metrics_server(port: int = 9090) -> bool:
    """Start the Prometheus metrics HTTP server.

    Only starts for the Prometheus backend. For Ray, metrics are
    exposed through Ray's built-in endpoint (no-op here).

    Args:
        port: TCP port for the metrics endpoint.

    Returns:
        True if the server started, False if skipped or port busy.
    """
    m = get_metrics()
    if isinstance(m, RayMetrics):
        logger.info(
            "metrics_server_skipped_ray",
            reason="Ray exposes metrics via its own endpoint",
        )
        return False

    try:
        from prometheus_client import start_http_server

        start_http_server(port)
        logger.info("metrics_server_started", port=port)
        return True
    except OSError:
        logger.warning("metrics_server_port_busy", port=port)
        return False


def record_package_completed(
    duration_seconds: float,
    num_sequences: int,
    num_hits: int,
) -> None:
    """Record a successfully completed work package."""
    m = get_metrics()
    m.observe_duration(duration_seconds)
    m.inc_sequences(num_sequences)
    m.inc_hits(num_hits)


def record_package_failed(error_type: str) -> None:
    """Record a failed work package."""
    get_metrics().inc_error(error_type)


def record_diamond_result(exit_code: int) -> None:
    """Record a DIAMOND execution result."""
    get_metrics().inc_diamond_exit(exit_code)


def update_package_states(status: dict[str, int]) -> None:
    """Update package state gauges from work stack status."""
    m = get_metrics()
    for state, count in status.items():
        m.set_package_state(state, count)


def inc_worker() -> None:
    """Increment the active worker count."""
    get_metrics().inc_worker()


def dec_worker() -> None:
    """Decrement the active worker count."""
    get_metrics().dec_worker()
