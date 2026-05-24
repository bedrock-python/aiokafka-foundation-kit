"""Kafka producer providers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Any

from aiokafka import AIOKafkaProducer

from aiokafka_foundation_kit.config.producer import ProducerLifecycleSettingsProtocol
from aiokafka_foundation_kit.config.topic import TopicConfigProtocol
from aiokafka_foundation_kit.producer.lifecycle import producer_lifecycle

from ._deps import Provider, Scope, check_dishka, provide


class AsyncKafkaProducerProvider(Provider):
    """Dishka provider exposing an ``AIOKafkaProducer`` for ``APP`` scope.

    Topics passed via the container are auto-created when
    ``settings.auto_create_topics`` is enabled.
    """

    scope = Scope.APP

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        check_dishka()
        super().__init__(*args, **kwargs)

    @provide
    async def get_kafka_producer(
        self,
        kafka_settings: ProducerLifecycleSettingsProtocol,
        topics: Sequence[TopicConfigProtocol] | None = None,
    ) -> AsyncIterator[AIOKafkaProducer]:
        """Provide Kafka producer as a singleton for APP scope."""
        async with producer_lifecycle(
            kafka_settings,
            topics=topics,
            auto_create_topics=kafka_settings.auto_create_topics,
        ) as producer:
            yield producer
