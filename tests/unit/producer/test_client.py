"""Unit tests for aiokafka_foundation_kit.producer.client."""

from __future__ import annotations

from unittest.mock import patch

from aiokafka_foundation_kit.producer.client import create_async_kafka_producer
from aiokafka_foundation_kit.utils.json import dumps_bytes

# ---------------------------------------------------------------------------
# Returns AIOKafkaProducer instance
# ---------------------------------------------------------------------------


def test__create_async_kafka_producer__minimal_settings__returns_producer_instance(
    producer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        result = create_async_kafka_producer(producer_settings)

    # Assert
    assert result is mock_cls.return_value


# ---------------------------------------------------------------------------
# Config keys forwarded correctly
# ---------------------------------------------------------------------------


def test__create_async_kafka_producer__settings__forwards_acks(producer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["acks"] == "all"


def test__create_async_kafka_producer__settings__forwards_compression_type(producer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["compression_type"] == "gzip"


def test__create_async_kafka_producer__settings__forwards_all_producer_fields(producer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["enable_idempotence"] is True
    assert kwargs["max_batch_size"] == 16384
    assert kwargs["linger_ms"] == 5
    assert kwargs["request_timeout_ms"] == 30000


def test__create_async_kafka_producer__settings__forwards_bootstrap_servers(producer_settings):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["bootstrap_servers"] == "localhost:9092"


# ---------------------------------------------------------------------------
# value_serializer
# ---------------------------------------------------------------------------


def test__create_async_kafka_producer__no_serializer__defaults_to_dumps_bytes(
    producer_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["value_serializer"] is dumps_bytes


def test__create_async_kafka_producer__custom_serializer__uses_custom(producer_settings):
    # Arrange
    custom_ser = lambda v: v  # noqa: E731
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings, value_serializer=custom_ser)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["value_serializer"] is custom_ser
