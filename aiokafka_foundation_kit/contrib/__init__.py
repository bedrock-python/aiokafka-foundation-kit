"""Optional integrations for aiokafka-foundation-kit.

This module contains optional integrations that require additional dependencies:

- models: Ready-to-use Pydantic models for Kafka configuration (requires pydantic)
- di: Dishka DI providers (requires dishka)
- dependency_injector: dependency-injector containers (requires dependency-injector)
- telemetry: OpenTelemetry instrumentation (requires opentelemetry-*)

Usage:
    from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings
    from aiokafka_foundation_kit.contrib.di import AsyncKafkaProducerProvider
    from aiokafka_foundation_kit.contrib.dependency_injector import KafkaProducerContainer
    from aiokafka_foundation_kit.contrib.telemetry import instrument_aiokafka
"""

__all__: list[str] = []  # Explicit imports required from submodules
