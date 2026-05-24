"""Kafka producer client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaProducer

from aiokafka_foundation_kit.config.producer import ProducerSettingsProtocol
from aiokafka_foundation_kit.utils.config import build_kafka_common_config
from aiokafka_foundation_kit.utils.json import dumps_bytes

if TYPE_CHECKING:
    from collections.abc import Callable


def create_async_kafka_producer(
    settings: ProducerSettingsProtocol,
    *,
    value_serializer: Callable[[Any], bytes] | None = None,
) -> AIOKafkaProducer:
    """Create an ``AIOKafkaProducer`` from settings.

    Args:
        settings: Kafka producer settings.
        value_serializer: Optional custom value serializer.
            Defaults to ``dumps_bytes`` (orjson when available, stdlib ``json``
            otherwise, with bytes pass-through).

    Returns:
        Configured ``AIOKafkaProducer`` (not started).
    """
    producer_config: dict[str, Any] = {
        **build_kafka_common_config(settings),
        "acks": settings.acks,
        "compression_type": settings.compression_type,
        "enable_idempotence": settings.enable_idempotence,
        "max_batch_size": settings.max_batch_size,
        "linger_ms": settings.linger_ms,
        "request_timeout_ms": settings.request_timeout_ms,
        "value_serializer": value_serializer or dumps_bytes,
    }

    return AIOKafkaProducer(**producer_config)
