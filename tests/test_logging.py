"""Tests for the centralised structlog configuration."""

from __future__ import annotations

import json
import logging
from io import StringIO

import structlog

from distributed_alignment.observability.logging import configure_logging


def _capture_log_output(
    *,
    level: str = "INFO",
    run_id: str | None = None,
    json_output: bool = True,
) -> tuple[StringIO, structlog.stdlib.BoundLogger]:
    """Configure logging to capture output in a StringIO buffer.

    Returns the buffer and a bound logger for emitting test events.
    """
    buf = StringIO()

    configure_logging(level=level, run_id=run_id, json_output=json_output)

    # Replace the root handler's stream with our buffer
    root = logging.getLogger()
    for handler in root.handlers:
        handler.stream = buf  # type: ignore[union-attr]

    log: structlog.stdlib.BoundLogger = structlog.get_logger("test")
    return buf, log


class TestJsonOutput:
    """Tests for JSON log output format."""

    def test_produces_valid_json(self) -> None:
        buf, log = _capture_log_output(json_output=True)

        log.info("test_event", key="value")

        output = buf.getvalue().strip()
        assert output, "No log output captured"
        parsed = json.loads(output)
        assert parsed["event"] == "test_event"
        assert parsed["key"] == "value"

    def test_includes_log_level(self) -> None:
        buf, log = _capture_log_output(json_output=True)

        log.info("test_event")

        parsed = json.loads(buf.getvalue().strip())
        assert parsed["level"] == "info"

    def test_includes_timestamp(self) -> None:
        buf, log = _capture_log_output(json_output=True)

        log.info("test_event")

        parsed = json.loads(buf.getvalue().strip())
        assert "timestamp" in parsed
        # ISO format contains 'T' separator
        assert "T" in parsed["timestamp"]


class TestRunId:
    """Tests for run_id correlation."""

    def test_run_id_in_output(self) -> None:
        buf, log = _capture_log_output(run_id="test-run-123", json_output=True)

        log.info("test_event")

        parsed = json.loads(buf.getvalue().strip())
        assert parsed["run_id"] == "test-run-123"

    def test_no_run_id_when_not_set(self) -> None:
        buf, log = _capture_log_output(run_id=None, json_output=True)

        log.info("test_event")

        parsed = json.loads(buf.getvalue().strip())
        assert "run_id" not in parsed

    def test_reconfigure_updates_run_id(self) -> None:
        buf, log = _capture_log_output(run_id="run-first", json_output=True)
        log.info("first_event")
        first_line = buf.getvalue().strip()

        # Reconfigure with a new run_id
        buf2 = StringIO()
        configure_logging(run_id="run-second", json_output=True)
        root = logging.getLogger()
        for handler in root.handlers:
            handler.stream = buf2  # type: ignore[union-attr]

        log.info("second_event")
        second_line = buf2.getvalue().strip()

        assert json.loads(first_line)["run_id"] == "run-first"
        assert json.loads(second_line)["run_id"] == "run-second"


class TestBoundContext:
    """Tests for component and per-call context."""

    def test_component_binding(self) -> None:
        buf, _ = _capture_log_output(json_output=True)

        log = structlog.get_logger(component="merger")
        log.info("merge_started")

        parsed = json.loads(buf.getvalue().strip())
        assert parsed["component"] == "merger"

    def test_per_call_context(self) -> None:
        buf, log = _capture_log_output(json_output=True)

        log.info("test_event", package_id="wp_q000_r000", worker_id="w1")

        parsed = json.loads(buf.getvalue().strip())
        assert parsed["package_id"] == "wp_q000_r000"
        assert parsed["worker_id"] == "w1"

    def test_bind_adds_persistent_context(self) -> None:
        buf, log = _capture_log_output(json_output=True)

        bound = log.bind(package_id="wp_q000_r000")
        bound.info("first")
        first_line = buf.getvalue().strip().split("\n")[-1]

        bound.info("second")
        second_line = buf.getvalue().strip().split("\n")[-1]

        assert json.loads(first_line)["package_id"] == "wp_q000_r000"
        assert json.loads(second_line)["package_id"] == "wp_q000_r000"


class TestLogLevels:
    """Tests for log level filtering."""

    def test_debug_hidden_at_info_level(self) -> None:
        buf, log = _capture_log_output(level="INFO", json_output=True)

        log.debug("should_not_appear")
        log.info("should_appear")

        lines = [line for line in buf.getvalue().strip().split("\n") if line]
        assert len(lines) == 1
        assert json.loads(lines[0])["event"] == "should_appear"

    def test_debug_visible_at_debug_level(self) -> None:
        buf, log = _capture_log_output(level="DEBUG", json_output=True)

        log.debug("should_appear")

        lines = [line for line in buf.getvalue().strip().split("\n") if line]
        assert len(lines) == 1
        assert json.loads(lines[0])["event"] == "should_appear"

    def test_warning_visible_at_info_level(self) -> None:
        buf, log = _capture_log_output(level="INFO", json_output=True)

        log.warning("warn_event")

        lines = [line for line in buf.getvalue().strip().split("\n") if line]
        assert len(lines) == 1
        assert json.loads(lines[0])["level"] == "warning"


class TestIdempotency:
    """Tests that reconfiguration doesn't stack handlers."""

    def test_no_duplicate_handlers(self) -> None:
        configure_logging(level="INFO", json_output=True)
        configure_logging(level="INFO", json_output=True)
        configure_logging(level="INFO", json_output=True)

        root = logging.getLogger()
        assert len(root.handlers) == 1
