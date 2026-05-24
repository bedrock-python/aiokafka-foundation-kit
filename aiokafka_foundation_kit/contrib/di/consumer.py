"""Kafka consumer providers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer

from aiokafka_foundation_kit.config.consumer import ConsumerSettingsProtocol
from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle

from ._deps import Provider, Scope, check_dishka, provide


class AsyncKafkaConsumerProvider(Provider):
    """Dishka provider exposing an ``AIOKafkaConsumer`` for ``APP`` scope."""

    scope = Scope.APP

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        check_dishka()
        super().__init__(*args, **kwargs)

    @provide
    async def get_kafka_consumer(
        self,
        kafka_settings: ConsumerSettingsProtocol,
        topics: tuple[str, ...] | None = None,
    ) -> AsyncIterator[AIOKafkaConsumer]:
        """Provide Kafka consumer as a singleton for APP scope."""
        async with consumer_lifecycle(
            kafka_settings,
            topics=topics,
        ) as consumer:
            yield consumer
