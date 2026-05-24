"""Unit tests for aiokafka_foundation_kit.contrib.models.consumer."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aiokafka_foundation_kit.contrib.models.consumer import BaseKafkaConsumerSettings

# ---------------------------------------------------------------------------
# Minimal valid creation
# ---------------------------------------------------------------------------


def test__base_kafka_consumer_settings__minimal_required__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="localhost:9092",
        group_id="my-group",
    )

    # Assert
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.group_id == "my-group"


def test__base_kafka_consumer_settings__defaults__are_correct():
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="b:9092",
        group_id="grp",
    )

    # Assert
    assert settings.auto_offset_reset == "earliest"
    assert settings.enable_auto_commit is False
    assert settings.session_timeout_ms == 30000
    assert settings.heartbeat_interval_ms == 3000
    assert settings.max_poll_records == 500
    assert settings.max_poll_interval_ms == 300000
    assert settings.fetch_max_wait_ms == 500
    assert settings.fetch_min_bytes == 1
    assert settings.fetch_max_bytes == 52428800


# ---------------------------------------------------------------------------
# group_id required
# ---------------------------------------------------------------------------


def test__base_kafka_consumer_settings__missing_group_id__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError):
        BaseKafkaConsumerSettings(bootstrap_servers="b:9092")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# auto_offset_reset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("reset", ["earliest", "latest"])
def test__base_kafka_consumer_settings__valid_offset_reset__accepted(reset: str):
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="b:9092",
        group_id="g",
        auto_offset_reset=reset,  # type: ignore[arg-type]
    )

    # Assert
    assert settings.auto_offset_reset == reset


# ---------------------------------------------------------------------------
# Custom overrides
# ---------------------------------------------------------------------------


def test__base_kafka_consumer_settings__custom_max_poll_records__stored_correctly():
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="b:9092",
        group_id="g",
        max_poll_records=100,
    )

    # Assert
    assert settings.max_poll_records == 100


def test__base_kafka_consumer_settings__enable_auto_commit_true__accepted():
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="b:9092",
        group_id="g",
        enable_auto_commit=True,
    )

    # Assert
    assert settings.enable_auto_commit is True


# ---------------------------------------------------------------------------
# Inherits from BaseKafkaSettings
# ---------------------------------------------------------------------------


def test__base_kafka_consumer_settings__inherits_security_protocol_default():
    # Arrange / Act
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers="b:9092",
        group_id="g",
    )

    # Assert
    assert settings.security_protocol == "PLAINTEXT"
