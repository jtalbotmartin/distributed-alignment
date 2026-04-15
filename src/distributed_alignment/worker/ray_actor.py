"""Ray actor wrapper for the alignment worker.

Ray manages process lifecycle and resource allocation; the actual
work is done by the existing WorkerRunner. The actor is a thin
wrapper that reconstructs all dependencies inside the Ray process
(logging, work stack, DIAMOND wrapper) from plain-dict config.
"""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(component="ray_actor")


def _try_import_ray() -> Any:  # noqa: ANN401
    """Import ray, raising a clear error if not installed."""
    try:
        import ray

        return ray
    except ImportError:
        msg = "Ray is not installed. Install with: uv add 'distributed-alignment[ray]'"
        raise ImportError(msg) from None


def create_alignment_actor(
    worker_config: dict[str, Any],
    *,
    num_cpus: int = 1,
) -> Any:  # noqa: ANN401
    """Create a Ray remote AlignmentWorker actor.

    Imports Ray at call time so the module can be imported without
    Ray installed (the error is raised only when Ray is actually used).

    Args:
        worker_config: Plain dict with worker configuration. Keys match
            ``run_worker_process()`` keyword arguments.
        num_cpus: Number of CPUs to reserve for this actor.

    Returns:
        A Ray actor handle.
    """
    ray = _try_import_ray()

    @ray.remote(num_cpus=num_cpus)
    class AlignmentWorker:
        """Ray actor that runs a WorkerRunner in a remote process."""

        def __init__(self, config: dict[str, Any]) -> None:
            self._config = config

        def run(self) -> dict[str, Any]:
            """Execute the worker loop and return a summary."""
            from pathlib import Path

            from distributed_alignment.observability.logging import (
                configure_logging,
            )
            from distributed_alignment.scheduler.filesystem_backend import (
                FileSystemWorkStack,
            )
            from distributed_alignment.worker.diamond_wrapper import (
                DiamondWrapper,
            )
            from distributed_alignment.worker.runner import WorkerRunner

            cfg = self._config

            configure_logging(
                level=str(cfg.get("log_level", "INFO")),
                run_id=str(cfg.get("run_id", "")),
                json_output=True,
            )

            try:
                stack = FileSystemWorkStack(Path(str(cfg["work_stack_dir"])))
                diamond = DiamondWrapper(
                    binary=str(cfg.get("diamond_binary", "diamond")),
                    threads=1,
                )

                runner = WorkerRunner(
                    stack,
                    diamond,
                    Path(str(cfg["chunks_dir"])),
                    Path(str(cfg["results_dir"])),
                    sensitivity=str(cfg.get("sensitivity", "very-sensitive")),
                    max_target_seqs=int(str(cfg.get("max_target_seqs", 50))),
                    timeout=int(str(cfg.get("timeout", 3600))),
                    heartbeat_interval=float(str(cfg.get("heartbeat_interval", 30.0))),
                    heartbeat_timeout=int(str(cfg.get("heartbeat_timeout", 120))),
                    reaper_interval=float(str(cfg.get("reaper_interval", 60.0))),
                    max_idle_time=float(str(cfg.get("max_idle_time", 30.0))),
                    metrics_port=int(str(cfg.get("metrics_port", 9090))),
                )

                completed = runner.run()

                return {
                    "worker_id": runner.worker_id,
                    "packages_completed": completed,
                    "error": None,
                }
            except Exception as exc:
                return {
                    "worker_id": "unknown",
                    "packages_completed": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                }

    return AlignmentWorker.remote(worker_config)  # type: ignore[attr-defined]


def run_ray_workers(
    worker_config: dict[str, Any],
    *,
    num_workers: int,
    num_cpus_per_worker: int = 1,
) -> list[dict[str, Any]]:
    """Spawn Ray actors and wait for all to complete.

    Initialises Ray in local mode (or connects to an existing cluster
    if ``RAY_ADDRESS`` is set), creates ``num_workers`` actors, runs
    them, collects results, and shuts down Ray.

    Args:
        worker_config: Plain dict with worker configuration.
        num_workers: Number of actors to spawn.
        num_cpus_per_worker: CPUs per actor.

    Returns:
        List of result dicts from each actor.
    """
    ray = _try_import_ray()

    try:
        import os

        # Remove Ray's uv runtime env hook which conflicts with
        # uv run invocations. Must be deleted (not set empty).
        os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)

        # Set PYTHONPATH for Ray workers to find src/
        src_path = os.path.join(os.getcwd(), "src")

        ray.init(
            num_cpus=num_workers * num_cpus_per_worker,
            ignore_reinit_error=True,
            runtime_env={
                "env_vars": {"PYTHONPATH": src_path},
            },
        )

        cluster_info = ray.cluster_resources()
        logger.info(
            "ray_cluster_info",
            num_cpus=cluster_info.get("CPU", 0),
            num_workers=num_workers,
        )

        actors = [
            create_alignment_actor(worker_config, num_cpus=num_cpus_per_worker)
            for _ in range(num_workers)
        ]

        futures = [actor.run.remote() for actor in actors]
        results: list[dict[str, Any]] = ray.get(futures)

        return results

    finally:
        ray.shutdown()
