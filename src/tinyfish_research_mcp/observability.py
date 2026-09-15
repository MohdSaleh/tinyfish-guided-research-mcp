"""Vendor-neutral structured logging and OpenTelemetry bootstrap.

The MCP SDK v2 already emits protocol-level spans. This module adds a minimal
application layer that can export through OTLP when the optional observability
extra is installed. When it is not installed, tracing remains a safe no-op.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator

_LOGGER_NAME = "tinyfish_research_mcp"
_LOGGING_CONFIGURED = False
_OTEL_CONFIGURED = False


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "research_id",
            "claim_id",
            "source_id",
            "work_id",
            "tool",
            "duration_ms",
            "attempt",
            "error_type",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> logging.Logger:
    global _LOGGING_CONFIGURED
    logger = logging.getLogger(_LOGGER_NAME)
    if _LOGGING_CONFIGURED:
        return logger

    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stderr)  # stdout is reserved for stdio MCP traffic.
    if os.environ.get("LOG_FORMAT", "json").lower() == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    _LOGGING_CONFIGURED = True
    return logger


logger = configure_logging()

try:
    from opentelemetry import trace  # type: ignore
except Exception:  # pragma: no cover - OpenTelemetry API is optional outside MCP runtime
    trace = None


def configure_observability() -> None:
    """Configure logging and optional OTLP tracing exactly once.

    OTLP configuration is activated only when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is
    set. This keeps local stdio usage dependency-light while allowing standard
    collectors/backends in production without vendor-specific code.
    """
    global _OTEL_CONFIGURED
    configure_logging()
    if _OTEL_CONFIGURED or trace is None:
        return

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        _OTEL_CONFIGURED = True
        return

    try:  # optional dependency group: [observability]
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        provider = TracerProvider(
            resource=Resource.create(
                {
                    "service.name": os.environ.get("OTEL_SERVICE_NAME", "tinyfish-guided-research-mcp"),
                    "service.version": os.environ.get("SERVICE_VERSION", "1.0.0"),
                }
            )
        )
        exporter = OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces")
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        logger.info("opentelemetry_configured")
    except Exception:
        logger.exception("opentelemetry_configuration_failed")
    finally:
        _OTEL_CONFIGURED = True


@contextmanager
def span(name: str, **attributes: Any) -> Iterator[Any]:
    if trace is None:
        yield None
        return

    tracer = trace.get_tracer(_LOGGER_NAME)
    with tracer.start_as_current_span(name) as current:
        for key, value in attributes.items():
            if value is not None and isinstance(value, (str, bool, int, float)):
                current.set_attribute(key, value)
        yield current


def log_event(event: str, **fields: Any) -> None:
    logger.info(event, extra=fields)
