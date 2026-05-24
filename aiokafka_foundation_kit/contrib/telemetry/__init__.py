"""OpenTelemetry instrumentation for aiokafka."""

from __future__ import annotations

from .instrumentations import instrument_aiokafka

__all__ = ["instrument_aiokafka"]
