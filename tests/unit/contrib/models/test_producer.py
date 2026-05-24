"""Unit tests for aiokafka_foundation_kit.contrib.models.producer."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aiokafka_foundation_kit.contrib.models.producer import (
    BaseKafkaProducerSettings,
    KafkaAutoCreateMixin,
)

# ---------------------------------------------------------------------------
# Minimal valid creation
# ---------------------------------------------------------------------------


def test__base_kafka_producer_settings__minimal__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaProducerSettings(bootstrap_servers="localhost:9092")

    # Assert
    assert settings.bootstrap_servers == "localhost:9092"


def test__base_kafka_producer_settings__defaults__are_correct():
    # Arrange / Act
    settings = BaseKafkaProducerSettings(bootstrap_servers="b:9092")

    # Assert
    assert settings.acks == "all"
    assert settings.compression_type == "gzip"
    assert settings.enable_idempotence is True
    assert settings.max_batch_size == 16384
    assert settings.linger_ms == 5
    assert settings.request_timeout_ms == 30000
    assert settings.auto_create_topics is False
    assert settings.default_partitions == 3
    assert settings.default_replication_factor is None


# ---------------------------------------------------------------------------
# acks values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("acks", ["0", "1", "all"])
def test__base_kafka_producer_settings__valid_acks__accepted(acks: str):
    # Arrange / Act
    settings = BaseKafkaProducerSettings(
        bootstrap_servers="b:9092",
        acks=acks,  # type: ignore[arg-type]
    )

    # Assert
    assert settings.acks == acks


# ---------------------------------------------------------------------------
# compression_type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ct", ["gzip", "snappy", "lz4", "zstd", None])
def test__base_kafka_producer_settings__compression_types__accepted(ct):
    # Arrange / Act
    settings = BaseKafkaProducerSettings(
        bootstrap_servers="b:9092",
        compression_type=ct,
    )

    # Assert
    assert settings.compression_type == ct


# ---------------------------------------------------------------------------
# auto_create_topics validation
# ---------------------------------------------------------------------------


def test__base_kafka_producer_settings__auto_create_true_without_replication__raises():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="default_replication_factor"):
        BaseKafkaProducerSettings(
            bootstrap_servers="b:9092",
            auto_create_topics=True,
        )


def test__base_kafka_producer_settings__auto_create_true_with_replication__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaProducerSettings(
        bootstrap_servers="b:9092",
        auto_create_topics=True,
        default_replication_factor=2,
    )

    # Assert
    assert settings.auto_create_topics is True
    assert settings.default_replication_factor == 2


def test__base_kafka_producer_settings__auto_create_false_no_replication__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaProducerSettings(
        bootstrap_servers="b:9092",
        auto_create_topics=False,
    )

    # Assert
    assert settings.auto_create_topics is False
    assert settings.default_replication_factor is None


# ---------------------------------------------------------------------------
# KafkaAutoCreateMixin standalone
# ---------------------------------------------------------------------------


def test__kafka_auto_create_mixin__defaults__auto_create_false():
    # Arrange / Act
    mixin = KafkaAutoCreateMixin()

    # Assert
    assert mixin.auto_create_topics is False
    assert mixin.default_partitions == 3
    assert mixin.default_replication_factor is None


# ---------------------------------------------------------------------------
# Inherits SSL/SASL validation from BaseKafkaSettings
# ---------------------------------------------------------------------------


def test__base_kafka_producer_settings__sasl_plaintext_missing_mechanism__raises():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="sasl_mechanism"):
        BaseKafkaProducerSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_username="u",
            sasl_password="p",  # type: ignore[arg-type]
        )
