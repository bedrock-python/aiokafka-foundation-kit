"""Unit tests for aiokafka_foundation_kit.producer.client."""

from __future__ import annotations

from typing import get_args
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaProducer

from aiokafka_foundation_kit.config.kafka import KafkaAcks
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
# acks — KafkaAcks holds strings, aiokafka takes 0, 1, -1 or "all"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("acks", "expected"), [("0", 0), ("1", 1), ("all", "all")])
def test__create_async_kafka_producer__acks__translated_for_aiokafka(producer_settings, acks: str, expected: object):
    # Arrange
    producer_settings.acks = acks
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["acks"] == expected
    assert type(kwargs["acks"]) is type(expected)


@pytest.mark.parametrize("acks", get_args(KafkaAcks))
async def test__create_async_kafka_producer__every_kafka_acks_value__constructs_real_producer(
    producer_settings, acks: str
):
    # Arrange — idempotence off, so aiokafka accepts every acks level
    producer_settings.acks = acks
    producer_settings.enable_idempotence = False

    # Act
    result = create_async_kafka_producer(producer_settings)

    # Assert
    assert isinstance(result, AIOKafkaProducer)
    await result.stop()


@pytest.mark.parametrize("acks", ["0", "1"])
async def test__create_async_kafka_producer__acks_below_all_with_idempotence__aiokafka_rejects(
    producer_settings, acks: str
):
    # Arrange — the default enable_idempotence=True allows only acks="all"
    producer_settings.acks = acks

    # Act / Assert
    with pytest.raises(ValueError, match=f"acks={acks} not supported if enable_idempotence=True"):
        create_async_kafka_producer(producer_settings)


def test__create_async_kafka_producer__acks_already_an_int__forwarded_unchanged(producer_settings):
    # Arrange — a hand-rolled settings object that stores aiokafka's own value
    producer_settings.acks = -1
    with patch("aiokafka_foundation_kit.producer.client.AIOKafkaProducer") as mock_cls:
        # Act
        create_async_kafka_producer(producer_settings)

    # Assert
    _, kwargs = mock_cls.call_args
    assert kwargs["acks"] == -1


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


# ---------------------------------------------------------------------------
# TLS — aiokafka only accepts a prepared ssl_context
# ---------------------------------------------------------------------------


async def test__create_async_kafka_producer__ssl_settings__constructs_real_producer(producer_settings):
    # Arrange — no cafile, so the system trust store is used and no file is read
    producer_settings.security_protocol = "SSL"
    producer_settings.ssl_cafile = None

    # Act
    result = create_async_kafka_producer(producer_settings)

    # Assert
    assert isinstance(result, AIOKafkaProducer)


async def test__create_async_kafka_producer__sasl_ssl_settings__constructs_real_producer(producer_settings):
    # Arrange
    producer_settings.security_protocol = "SASL_SSL"
    producer_settings.sasl_mechanism = "PLAIN"
    producer_settings.sasl_username = "user"
    producer_settings.get_sasl_password.return_value = "pw"
    producer_settings.ssl_cafile = None

    # Act
    result = create_async_kafka_producer(producer_settings)

    # Assert
    assert isinstance(result, AIOKafkaProducer)
