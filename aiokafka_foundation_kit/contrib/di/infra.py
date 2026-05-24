"""Kafka infrastructure providers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from aiokafka_foundation_kit.topics.config import TopicConfig

from ._deps import Provider, Scope, check_dishka, provide


class KafkaTopicSettingsProtocol(Protocol):
    """Physical parameters for one topic in a catalog.

    Unlike :class:`~aiokafka_foundation_kit.config.topic.TopicConfigProtocol`,
    this one omits ``name`` because the topic name comes from the catalog map
    key in :class:`KafkaProducerInfraSettingsProtocol.topic_catalog`.
    """

    num_partitions: int
    replication_factor: int
    topic_configs: dict[str, str] | None


class KafkaInfraBaseSettingsProtocol(Protocol):
    """Shared base for producer and consumer Kafka infrastructure settings."""

    topic_prefix: str | None


class KafkaProducerInfraSettingsProtocol(KafkaInfraBaseSettingsProtocol, Protocol):
    """Protocol for producer-side Kafka infrastructure settings.

    Defines what producer providers need: a topic prefix and a catalog of
    physical topic configs to ensure exist on startup.
    """

    topic_catalog: dict[str, KafkaTopicSettingsProtocol] | None


class KafkaConsumerInfraSettingsProtocol(KafkaInfraBaseSettingsProtocol, Protocol):
    """Protocol for consumer-side Kafka infrastructure settings.

    Defines what consumer providers need: a topic prefix and a list of
    logical topic names to subscribe to.
    """

    consumer_subscriptions: list[str] | None


def _apply_topic_prefix(prefix: str | None, name: str) -> str:
    return f"{prefix}.{name}" if prefix else name


class KafkaInfraProvider(Provider):
    """Provide resolved topic configs for producer and subscriptions for consumers."""

    scope = Scope.APP

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        check_dishka()
        super().__init__(*args, **kwargs)

    @provide
    def get_topic_configs_for_catalog(
        self,
        settings: KafkaProducerInfraSettingsProtocol,
    ) -> Sequence[TopicConfig]:
        """Provide physical topic configs for topic creation based on topic catalog."""
        prefix = settings.topic_prefix
        topic_catalog = settings.topic_catalog or {}

        return [
            TopicConfig(
                name=_apply_topic_prefix(prefix, name),
                num_partitions=cfg.num_partitions,
                replication_factor=cfg.replication_factor,
                topic_configs=cfg.topic_configs,
            )
            for name, cfg in topic_catalog.items()
        ]

    @provide
    def get_consumer_subscription_topics(
        self,
        settings: KafkaConsumerInfraSettingsProtocol,
    ) -> tuple[str, ...]:
        """Provide fully resolved physical topic names for consumer subscriptions."""
        subscriptions = settings.consumer_subscriptions or []
        if not subscriptions:
            return ()

        prefix = settings.topic_prefix
        return tuple(_apply_topic_prefix(prefix, topic) for topic in subscriptions)
