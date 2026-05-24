"""Unit tests for aiokafka_foundation_kit.contrib.models.infra."""

from __future__ import annotations

import pytest

from aiokafka_foundation_kit.contrib.models.infra import (
    BaseKafkaInfraSettings,
    KafkaTopicSettings,
    normalize_kafka_topic_prefix_value,
)

# ---------------------------------------------------------------------------
# normalize_kafka_topic_prefix_value
# ---------------------------------------------------------------------------


def test__normalize_kafka_topic_prefix_value__none__returns_none():
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value(None)

    # Assert
    assert result is None


@pytest.mark.parametrize(
    "value",
    ["None", "none", "NONE", "null", "NULL", "Null", "", "  ", "\t", "\n"],
)
def test__normalize_kafka_topic_prefix_value__null_like_strings__returns_none(value: str):
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value(value)

    # Assert
    assert result is None


def test__normalize_kafka_topic_prefix_value__valid_prefix__returns_stripped():
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value("  my-prefix  ")

    # Assert
    assert result == "my-prefix"


def test__normalize_kafka_topic_prefix_value__valid_prefix_no_whitespace__returns_as_is():
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value("prod")

    # Assert
    assert result == "prod"


def test__normalize_kafka_topic_prefix_value__non_string_non_none__returns_none():
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value(123)  # type: ignore[arg-type]

    # Assert
    assert result is None


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        ("None", None),
        ("null", None),
        ("  ", None),
        ("prod", "prod"),
        ("  dev  ", "dev"),
        ("staging.service", "staging.service"),
    ],
)
def test__normalize_kafka_topic_prefix_value__parametrized_inputs__correct_output(value: object, expected: str | None):
    # Arrange / Act
    result = normalize_kafka_topic_prefix_value(value)

    # Assert
    assert result == expected


# ---------------------------------------------------------------------------
# KafkaTopicSettings
# ---------------------------------------------------------------------------


def test__kafka_topic_settings__defaults__are_correct():
    # Arrange / Act
    s = KafkaTopicSettings()

    # Assert
    assert s.num_partitions == 3
    assert s.replication_factor == 3
    assert s.topic_configs is None


def test__kafka_topic_settings__custom_values__stored_correctly():
    # Arrange / Act
    s = KafkaTopicSettings(num_partitions=6, replication_factor=2, topic_configs={"retention.ms": "3600000"})

    # Assert
    assert s.num_partitions == 6
    assert s.replication_factor == 2
    assert s.topic_configs == {"retention.ms": "3600000"}


# ---------------------------------------------------------------------------
# BaseKafkaInfraSettings
# ---------------------------------------------------------------------------


def test__base_kafka_infra_settings__defaults__are_none():
    # Arrange / Act
    s = BaseKafkaInfraSettings()

    # Assert
    assert s.topic_prefix is None
    assert s.topic_catalog is None
    assert s.consumer_subscriptions is None


def test__base_kafka_infra_settings__null_like_prefix__coerced_to_none():
    # Arrange / Act
    s = BaseKafkaInfraSettings(topic_prefix="None")  # type: ignore[arg-type]

    # Assert
    assert s.topic_prefix is None


def test__base_kafka_infra_settings__empty_string_prefix__coerced_to_none():
    # Arrange / Act
    s = BaseKafkaInfraSettings(topic_prefix="")

    # Assert
    assert s.topic_prefix is None


def test__base_kafka_infra_settings__valid_prefix__stored_stripped():
    # Arrange / Act
    s = BaseKafkaInfraSettings(topic_prefix="  prod  ")

    # Assert
    assert s.topic_prefix == "prod"


def test__base_kafka_infra_settings__with_topic_catalog__stored_correctly():
    # Arrange / Act
    catalog = {"events": KafkaTopicSettings(num_partitions=6)}
    s = BaseKafkaInfraSettings(topic_catalog=catalog)

    # Assert
    assert s.topic_catalog is not None
    assert "events" in s.topic_catalog


def test__base_kafka_infra_settings__with_consumer_subscriptions__stored_correctly():
    # Arrange / Act
    s = BaseKafkaInfraSettings(consumer_subscriptions=["events", "commands"])

    # Assert
    assert s.consumer_subscriptions == ["events", "commands"]
