"""Kafka producer containers for dependency-injector."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

from aiokafka import AIOKafkaProducer

from ...producer.lifecycle import producer_lifecycle
from ._deps import check_dependency_injector, containers, providers

if TYPE_CHECKING:
    from ...config.producer import ProducerSettingsProtocol
    from ...config.topic import TopicConfigProtocol


async def _producer_resource(
    kafka_settings: ProducerSettingsProtocol,
    topics: Sequence[TopicConfigProtocol] | None,
    auto_create_topics: bool,
) -> AsyncIterator[AIOKafkaProducer]:
    async with producer_lifecycle(
        kafka_settings,
        topics=topics,
        auto_create_topics=auto_create_topics,
    ) as producer:
        yield producer


class KafkaProducerContainer(containers.DeclarativeContainer):
    """Container for Kafka producer dependencies.

    Provides:
        - ``producer``: :class:`AIOKafkaProducer` with automatic lifecycle.

    Configuration:
        - ``kafka_settings``: Kafka producer settings (``ProducerSettingsProtocol``).
        - ``topics``: Optional list of topics to auto-create (``Sequence[TopicConfigProtocol]``).
        - ``auto_create_topics``: Whether to run ``ensure_topics_async`` on startup.
    """

    kafka_settings = providers.Dependency()  # type: ignore[var-annotated]
    topics = providers.Dependency(default=None)  # type: ignore[var-annotated]
    auto_create_topics = providers.Object(False)  # type: ignore[var-annotated]

    producer = providers.Resource(  # type: ignore[var-annotated]
        _producer_resource,
        kafka_settings=kafka_settings,
        topics=topics,
        auto_create_topics=auto_create_topics,
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        check_dependency_injector()  # pragma: no cover
        super().__init__(*args, **kwargs)  # pragma: no cover
