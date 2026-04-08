"""Centralised structlog configuration for the distributed-alignment pipeline.

Provides JSON output for production and human-readable coloured output for
development. Binds ``run_id`` to all loggers as a correlation ID.
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(
    *,
    level: str = "INFO",
    run_id: str | None = None,
    json_output: bool | None = None,
) -> None:
    """Configure structlog for the pipeline.

    Safe to call multiple times — reconfigures processors and updates
    the bound ``run_id`` without stacking duplicate processors.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR).
        run_id: Pipeline run identifier, bound to all log entries.
            If None, no run_id is bound.
        json_output: If True, output JSON. If False, output coloured
            human-readable text. If None (default), auto-detect based
            on whether stdout is a TTY.
    """
    if json_output is None:
        json_output = not sys.stdout.isatty()

    # Shared processors for both structlog and stdlib integration
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if json_output:
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    # Configure structlog
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=False,
    )

    # Configure stdlib logging to use structlog formatting
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, level.upper()))

    # Bind run_id globally via contextvars so it appears in every entry
    structlog.contextvars.clear_contextvars()
    if run_id is not None:
        structlog.contextvars.bind_contextvars(run_id=run_id)
