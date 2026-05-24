"""Unit tests for aiokafka_foundation_kit.consumer.client."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from aiokafka_foundation_kit.consumer.client import create_async_kafka_consumer
from aiokafka_foundation_kit.utils.json import loads_bytes

# ---------------------------------------------------------------------------
# Returns AIOKafkaConsumer instance
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__minimal_settings__returns_consumer_instance(
    consumer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        result = create_async_kafka_consumer(consumer_settings)

    # Assert
    assert result is mock_cls.return_value


# ---------------------------------------------------------------------------
# No topics — no positional args
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__no_topics__called_with_empty_topic_args(
    consumer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings, topics=None)

    # Assert — first positional args are the topic tuple (empty)
    args, _ = mock_cls.call_args
    assert args == ()


def test__create_async_kafka_consumer__empty_topics_list__called_with_empty_topic_args(
    consumer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings, topics=[])

    # Assert
    args, _ = mock_cls.call_args
    assert args == ()


# ---------------------------------------------------------------------------
# With topics — topics passed as positional args
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__with_topics__topics_passed_as_positional_args(
    consumer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings, topics=["t1", "t2"])

    # Assert
    args, _ = mock_cls.call_args
    assert args == ("t1", "t2")


@pytest.mark.parametrize(
    "topics",
    [
        ["single-topic"],
        ["t1", "t2"],
        ["a", "b", "c"],
    ],
)
def test__create_async_kafka_consumer__various_topic_lists__converted_to_tuple_args(
    consumer_settings, topics: list[str]
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings, topics=topics)

    # Assert
    args, _ = mock_cls.call_args
    assert args == tuple(topics)


# ---------------------------------------------------------------------------
# Config keys forwarded correctly
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__settings__forwards_group_id(consumer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["group_id"] == "test-group"


def test__create_async_kafka_consumer__settings__forwards_auto_offset_reset(consumer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["auto_offset_reset"] == "earliest"


def test__create_async_kafka_consumer__settings__forwards_all_consumer_fields(consumer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["session_timeout_ms"] == 30000
    assert kwargs["heartbeat_interval_ms"] == 3000
    assert kwargs["max_poll_records"] == 500
    assert kwargs["max_poll_interval_ms"] == 300000
    assert kwargs["fetch_max_wait_ms"] == 500
    assert kwargs["fetch_min_bytes"] == 1
    assert kwargs["fetch_max_bytes"] == 52428800


# ---------------------------------------------------------------------------
# value_deserializer
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__no_deserializer__defaults_to_loads_bytes(
    consumer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["value_deserializer"] is loads_bytes


def test__create_async_kafka_consumer__custom_deserializer__uses_custom(consumer_settings):
    # Arrange
    custom_deser = lambda v: v  # noqa: E731
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings, value_deserializer=custom_deser)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["value_deserializer"] is custom_deser


# ---------------------------------------------------------------------------
# Bootstrap servers from common config
# ---------------------------------------------------------------------------


def test__create_async_kafka_consumer__settings__forwards_bootstrap_servers(consumer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.consumer.client.AIOKafkaConsumer") as mock_cls:
        # Act
        create_async_kafka_consumer(consumer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["bootstrap_servers"] == "localhost:9092"
