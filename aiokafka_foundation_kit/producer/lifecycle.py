"""Shared Kafka producer lifecycle for DI providers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from aiokafka_foundation_kit.producer.client import create_async_kafka_producer
from aiokafka_foundation_kit.topics.management import ensure_topics_async
from aiokafka_foundation_kit.utils.lifecycle import managed_kafka_client

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Sequence

    from aiokafka import AIOKafkaProducer

    from aiokafka_foundation_kit.config.producer import ProducerSettingsProtocol
    from aiokafka_foundation_kit.config.topic import TopicConfigProtocol


@asynccontextmanager
async def producer_lifecycle(
    settings: ProducerSettingsProtocol,
    *,
    topics: Sequence[TopicConfigProtocol] | None = None,
    auto_create_topics: bool = False,
    name: str = "producer",
    on_started: Callable[[AIOKafkaProducer], Awaitable[None]] | None = None,
    on_stopped: Callable[[AIOKafkaProducer], Awaitable[None]] | None = None,
) -> AsyncIterator[AIOKafkaProducer]:
    """Manage a Kafka producer's lifecycle as an async context.

    Auto-creates ``topics`` when ``auto_create_topics`` is true and ``topics``
    is non-empty, then starts the producer, yields it, and ensures it is stopped
    even if the caller raises.

    Args:
        settings: Kafka producer settings.
        topics: Optional topic configs to ensure exist before the producer starts.
        auto_create_topics: When true and ``topics`` is provided, run
            ``ensure_topics_async`` before producer start.
        name: Client name used in log messages. Useful when running multiple producers.
        on_started: Optional async hook invoked after a successful start.
        on_stopped: Optional async hook invoked after a successful stop.
    """
    if auto_create_topics and topics:
        await ensure_topics_async(topics, settings)

    producer = create_async_kafka_producer(settings)
    async with managed_kafka_client(
        producer,
        name=name,
        on_started=on_started,
        on_stopped=on_stopped,
    ) as started:
        yield started
