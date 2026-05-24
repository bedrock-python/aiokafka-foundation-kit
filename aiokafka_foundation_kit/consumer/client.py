"""Kafka consumer client."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer

from aiokafka_foundation_kit.config.consumer import ConsumerSettingsProtocol
from aiokafka_foundation_kit.utils.config import build_kafka_common_config
from aiokafka_foundation_kit.utils.json import loads_bytes

if TYPE_CHECKING:
    from collections.abc import Callable


def create_async_kafka_consumer(
    settings: ConsumerSettingsProtocol,
    topics: list[str] | None = None,
    *,
    value_deserializer: Callable[[bytes | None], Any] | None = None,
) -> AIOKafkaConsumer:
    """Create an ``AIOKafkaConsumer`` from settings.

    ``settings.enable_auto_commit`` is forwarded as-is. Keep it ``False`` for
    at-least-once delivery (the caller commits manually) and set it ``True``
    only for fire-and-forget or idempotent consumers.

    Args:
        settings: Kafka consumer settings.
        topics: Optional list of topics to subscribe to.
        value_deserializer: Optional custom value deserializer.
            Defaults to ``loads_bytes`` (orjson when available, stdlib ``json``
            otherwise).

    Returns:
        Configured ``AIOKafkaConsumer`` (not started).
    """
    consumer_config: dict[str, Any] = {
        **build_kafka_common_config(settings),
        "group_id": settings.group_id,
        "auto_offset_reset": settings.auto_offset_reset,
        "enable_auto_commit": settings.enable_auto_commit,
        "session_timeout_ms": settings.session_timeout_ms,
        "heartbeat_interval_ms": settings.heartbeat_interval_ms,
        "max_poll_records": settings.max_poll_records,
        "max_poll_interval_ms": settings.max_poll_interval_ms,
        "fetch_max_wait_ms": settings.fetch_max_wait_ms,
        "fetch_min_bytes": settings.fetch_min_bytes,
        "fetch_max_bytes": settings.fetch_max_bytes,
        "value_deserializer": value_deserializer or loads_bytes,
    }

    topic_args = tuple(topics) if topics else ()
    return AIOKafkaConsumer(*topic_args, **consumer_config)
