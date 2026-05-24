"""Unit tests for aiokafka_foundation_kit.contrib.di.infra."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import aiokafka_foundation_kit.contrib.di._deps as deps_mod
from aiokafka_foundation_kit.contrib.di.infra import (
    KafkaInfraProvider,
    _apply_topic_prefix,
)
from aiokafka_foundation_kit.contrib.models.infra import KafkaTopicSettings

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
