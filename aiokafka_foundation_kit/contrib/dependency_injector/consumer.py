"""Kafka consumer containers for dependency-injector."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaConsumer

from ...consumer.lifecycle import consumer_lifecycle
from ._deps import check_dependency_injector, containers, providers

if TYPE_CHECKING:
    from ...config.consumer import ConsumerSettingsProtocol


async def _consumer_resource(
    kafka_settings: ConsumerSettingsProtocol,
    topics: tuple[str, ...] | None,
) -> AsyncIterator[AIOKafkaConsumer]:
    async with consumer_lifecycle(kafka_settings, topics=topics) as consumer:
        yield consumer


class KafkaConsumerContainer(containers.DeclarativeContainer):
    """Container for Kafka consumer dependencies.

    Provides:
        - ``consumer``: :class:`AIOKafkaConsumer` with automatic lifecycle.

    Configuration:
        - ``kafka_settings``: Kafka consumer settings (``ConsumerSettingsProtocol``).
        - ``topics``: Optional tuple of topic names to subscribe to.
    """

    kafka_settings = providers.Dependency()  # type: ignore[var-annotated]
    topics = providers.Dependency(default=None)  # type: ignore[var-annotated]

    consumer = providers.Resource(  # type: ignore[var-annotated]
        _consumer_resource,
        kafka_settings=kafka_settings,
        topics=topics,
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        check_dependency_injector()  # pragma: no cover
        super().__init__(*args, **kwargs)  # pragma: no cover
