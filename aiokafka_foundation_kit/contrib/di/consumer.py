"""Kafka consumer providers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer

from aiokafka_foundation_kit.config.consumer import ConsumerSettingsProtocol
from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle

from ._deps import Provider, Scope, check_dishka, provide


class AsyncKafkaConsumerProvider(Provider):
    """Dishka provider exposing an ``AIOKafkaConsumer`` for ``APP`` scope.

    Args:
        value_deserializer: Optional custom value deserializer for the consumer,
            forwarded to :func:`consumer_lifecycle`. Defaults to ``loads_bytes``.
            It is a constructor argument because dishka resolves by type, and a
            bare callable has none to resolve by.
    """

    scope = Scope.APP

    def __init__(
        self,
        *args: Any,
        value_deserializer: Callable[[bytes | None], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        check_dishka()
        super().__init__(*args, **kwargs)
        self._value_deserializer = value_deserializer

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
            value_deserializer=self._value_deserializer,
        ) as consumer:
            yield consumer
