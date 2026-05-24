"""Shared Kafka consumer lifecycle for DI providers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from aiokafka_foundation_kit.consumer.client import create_async_kafka_consumer
from aiokafka_foundation_kit.utils.lifecycle import managed_kafka_client

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from aiokafka import AIOKafkaConsumer

    from aiokafka_foundation_kit.config.consumer import ConsumerSettingsProtocol


@asynccontextmanager
async def consumer_lifecycle(
    settings: ConsumerSettingsProtocol,
    *,
    topics: tuple[str, ...] | None = None,
    name: str = "consumer",
    on_started: Callable[[AIOKafkaConsumer], Awaitable[None]] | None = None,
    on_stopped: Callable[[AIOKafkaConsumer], Awaitable[None]] | None = None,
) -> AsyncIterator[AIOKafkaConsumer]:
    """Manage a Kafka consumer's lifecycle as an async context.

    Starts the consumer, yields it, and ensures it is stopped even if the caller raises.

    Args:
        settings: Kafka consumer settings.
        topics: Optional tuple of topic names to subscribe to.
        name: Client name used in log messages. Useful when running multiple consumers.
        on_started: Optional async hook invoked after a successful start.
        on_stopped: Optional async hook invoked after a successful stop.
    """
    consumer = create_async_kafka_consumer(settings, list(topics) if topics else None)
    async with managed_kafka_client(
        consumer,
        name=name,
        on_started=on_started,
        on_stopped=on_stopped,
    ) as started:
        yield started
