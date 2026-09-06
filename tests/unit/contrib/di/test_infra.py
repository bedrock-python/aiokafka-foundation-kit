"""Unit tests for aiokafka_foundation_kit.contrib.di.infra."""

from __future__ import annotations

from collections.abc import Sequence
from unittest.mock import MagicMock

import pytest
from dishka import Provider, Scope, make_async_container, provide

import aiokafka_foundation_kit.contrib.di._deps as deps_mod
from aiokafka_foundation_kit.config.consumer import ConsumerSettingsProtocol
from aiokafka_foundation_kit.config.producer import ProducerLifecycleSettingsProtocol
from aiokafka_foundation_kit.config.topic import TopicConfigProtocol
from aiokafka_foundation_kit.contrib.di.consumer import AsyncKafkaConsumerProvider
from aiokafka_foundation_kit.contrib.di.infra import (
    KafkaConsumerInfraSettingsProtocol,
    KafkaInfraProvider,
    KafkaProducerInfraSettingsProtocol,
    _apply_topic_prefix,
)
from aiokafka_foundation_kit.contrib.di.producer import AsyncKafkaProducerProvider
from aiokafka_foundation_kit.contrib.models.consumer import BaseKafkaConsumerSettings
from aiokafka_foundation_kit.contrib.models.infra import BaseKafkaInfraSettings, KafkaTopicSettings
from aiokafka_foundation_kit.contrib.models.producer import BaseKafkaProducerSettings

# ---------------------------------------------------------------------------
# _apply_topic_prefix
# ---------------------------------------------------------------------------


def test__apply_topic_prefix__no_prefix__returns_name_unchanged():
    # Arrange / Act
    result = _apply_topic_prefix(None, "events")

    # Assert
    assert result == "events"


def test__apply_topic_prefix__with_prefix__returns_dotted_name():
    # Arrange / Act
    result = _apply_topic_prefix("prod", "events")

    # Assert
    assert result == "prod.events"


@pytest.mark.parametrize(
    "prefix,name,expected",
    [
        (None, "events", "events"),
        ("", "events", "events"),  # empty string is falsy
        ("prod", "events", "prod.events"),
        ("dev.svc", "commands", "dev.svc.commands"),
    ],
)
def test__apply_topic_prefix__parametrized__returns_expected(prefix: str | None, name: str, expected: str):
    # Arrange / Act
    result = _apply_topic_prefix(prefix, name)

    # Assert
    assert result == expected


# ---------------------------------------------------------------------------
# KafkaInfraProvider — check_dishka called on init
# ---------------------------------------------------------------------------


def test__kafka_infra_provider__dishka_not_installed__raises_on_init(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dishka"):
        KafkaInfraProvider()


# ---------------------------------------------------------------------------
# get_topic_configs_for_catalog
# ---------------------------------------------------------------------------


def test__kafka_infra_provider__get_topic_configs_for_catalog__no_catalog__returns_empty_list():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = None
    settings.topic_catalog = None

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_topic_configs_for_catalog(settings)

    # Assert
    assert list(result) == []


def test__kafka_infra_provider__get_topic_configs_for_catalog__empty_catalog__returns_empty_list():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = None
    settings.topic_catalog = {}

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_topic_configs_for_catalog(settings)

    # Assert
    assert list(result) == []


def test__kafka_infra_provider__get_topic_configs_for_catalog__single_topic_no_prefix__correct_name():
    # Arrange
    topic_cfg = KafkaTopicSettings(num_partitions=3, replication_factor=2)
    settings = MagicMock()
    settings.topic_prefix = None
    settings.topic_catalog = {"events": topic_cfg}

    provider = KafkaInfraProvider()

    # Act
    result = list(provider.get_topic_configs_for_catalog(settings))

    # Assert
    assert len(result) == 1
    assert result[0].name == "events"
    assert result[0].num_partitions == 3
    assert result[0].replication_factor == 2


def test__kafka_infra_provider__get_topic_configs_for_catalog__with_prefix__prefixed_name():
    # Arrange
    topic_cfg = KafkaTopicSettings(num_partitions=6)
    settings = MagicMock()
    settings.topic_prefix = "prod"
    settings.topic_catalog = {"commands": topic_cfg}

    provider = KafkaInfraProvider()

    # Act
    result = list(provider.get_topic_configs_for_catalog(settings))

    # Assert
    assert result[0].name == "prod.commands"


def test__kafka_infra_provider__get_topic_configs_for_catalog__topic_configs_forwarded():
    # Arrange
    topic_cfg = KafkaTopicSettings(topic_configs={"retention.ms": "3600000"})
    settings = MagicMock()
    settings.topic_prefix = None
    settings.topic_catalog = {"logs": topic_cfg}

    provider = KafkaInfraProvider()

    # Act
    result = list(provider.get_topic_configs_for_catalog(settings))

    # Assert
    assert result[0].topic_configs == {"retention.ms": "3600000"}


# ---------------------------------------------------------------------------
# get_consumer_subscription_topics
# ---------------------------------------------------------------------------


def test__kafka_infra_provider__get_consumer_subscription_topics__no_subscriptions__returns_empty_tuple():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = None
    settings.consumer_subscriptions = None

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_consumer_subscription_topics(settings)

    # Assert
    assert result == ()


def test__kafka_infra_provider__get_consumer_subscription_topics__empty_list__returns_empty_tuple():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = None
    settings.consumer_subscriptions = []

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_consumer_subscription_topics(settings)

    # Assert
    assert result == ()


def test__kafka_infra_provider__get_consumer_subscription_topics__no_prefix__returns_bare_names():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = None
    settings.consumer_subscriptions = ["events", "commands"]

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_consumer_subscription_topics(settings)

    # Assert
    assert result == ("events", "commands")


def test__kafka_infra_provider__get_consumer_subscription_topics__with_prefix__returns_prefixed_names():
    # Arrange
    settings = MagicMock()
    settings.topic_prefix = "staging"
    settings.consumer_subscriptions = ["events", "commands"]

    provider = KafkaInfraProvider()

    # Act
    result = provider.get_consumer_subscription_topics(settings)

    # Assert
    assert result == ("staging.events", "staging.commands")


# ---------------------------------------------------------------------------
# Composition — a container holding the infra and client providers must build
# ---------------------------------------------------------------------------


class _SettingsProvider(Provider):
    """Supplies the settings objects the kit's providers ask the container for."""

    scope = Scope.APP

    def __init__(self, infra_settings: BaseKafkaInfraSettings) -> None:
        super().__init__()
        self._infra_settings = infra_settings

    @provide
    def producer_settings(self) -> ProducerLifecycleSettingsProtocol:
        return BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

    @provide
    def consumer_settings(self) -> ConsumerSettingsProtocol:
        return BaseKafkaConsumerSettings(bootstrap_servers="localhost:9092", group_id="test-group")

    @provide
    def producer_infra_settings(self) -> KafkaProducerInfraSettingsProtocol:
        return self._infra_settings

    @provide
    def consumer_infra_settings(self) -> KafkaConsumerInfraSettingsProtocol:
        return self._infra_settings


def _make_container():
    infra_settings = BaseKafkaInfraSettings(
        topic_prefix="prod",
        topic_catalog={"orders": KafkaTopicSettings(num_partitions=6, replication_factor=2)},
        consumer_subscriptions=["orders"],
    )
    return make_async_container(
        KafkaInfraProvider(),
        AsyncKafkaProducerProvider(),
        AsyncKafkaConsumerProvider(),
        _SettingsProvider(infra_settings),
    )


async def test__kafka_infra_provider__with_client_providers__container_builds():
    # Arrange / Act — the graph is validated here
    container = _make_container()

    # Assert
    assert container is not None
    await container.close()


async def test__kafka_infra_provider__with_client_providers__resolves_producer_topic_configs():
    # Arrange
    container = _make_container()

    # Act
    try:
        topics = await container.get(Sequence[TopicConfigProtocol] | None)
    finally:
        await container.close()

    # Assert
    assert [topic.name for topic in topics] == ["prod.orders"]


async def test__kafka_infra_provider__with_client_providers__resolves_consumer_subscriptions():
    # Arrange
    container = _make_container()

    # Act
    try:
        subscriptions = await container.get(tuple[str, ...] | None)
    finally:
        await container.close()

    # Assert
    assert subscriptions == ("prod.orders",)
