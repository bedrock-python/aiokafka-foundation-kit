"""Unit tests for aiokafka_foundation_kit.contrib.models.kafka."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from aiokafka_foundation_kit.contrib.models.kafka import BaseKafkaSettings

# ---------------------------------------------------------------------------
# PLAINTEXT — minimal valid config
# ---------------------------------------------------------------------------


def test__base_kafka_settings__plaintext__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaSettings(bootstrap_servers="localhost:9092")

    # Assert
    assert settings.bootstrap_servers == "localhost:9092"
    assert settings.security_protocol == "PLAINTEXT"


def test__base_kafka_settings__plaintext__defaults_are_set():
    # Arrange / Act
    settings = BaseKafkaSettings(bootstrap_servers="b:9092")

    # Assert
    assert settings.client_id is None
    assert settings.metadata_max_age_ms == 300000
    assert settings.sasl_mechanism is None
    assert settings.sasl_username is None
    assert settings.sasl_password is None
    assert settings.ssl_cafile is None
    assert settings.ssl_certfile is None
    assert settings.ssl_keyfile is None
    assert settings.ssl_check_hostname is True


# ---------------------------------------------------------------------------
# get_sasl_password
# ---------------------------------------------------------------------------


def test__base_kafka_settings__get_sasl_password__no_password__returns_none():
    # Arrange
    settings = BaseKafkaSettings(bootstrap_servers="b:9092")

    # Act
    result = settings.get_sasl_password()

    # Assert
    assert result is None


def test__base_kafka_settings__get_sasl_password__with_sasl_ssl__returns_plaintext_password():
    # Arrange
    settings = BaseKafkaSettings(
        bootstrap_servers="b:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="user",
        sasl_password="s3cr3t",  # type: ignore[arg-type]
        ssl_cafile="/ca.pem",
    )

    # Act
    result = settings.get_sasl_password()

    # Assert
    assert result == "s3cr3t"


# ---------------------------------------------------------------------------
# SASL_PLAINTEXT — validation
# ---------------------------------------------------------------------------


def test__base_kafka_settings__sasl_plaintext_with_all_credentials__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaSettings(
        bootstrap_servers="b:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username="user",
        sasl_password="pass",  # type: ignore[arg-type]
    )

    # Assert
    assert settings.security_protocol == "SASL_PLAINTEXT"
    assert settings.sasl_mechanism == "PLAIN"


def test__base_kafka_settings__sasl_plaintext_missing_mechanism__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="sasl_mechanism"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_username="user",
            sasl_password="pass",  # type: ignore[arg-type]
        )


def test__base_kafka_settings__sasl_plaintext_missing_username__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="sasl_username"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_password="pass",  # type: ignore[arg-type]
        )


def test__base_kafka_settings__sasl_plaintext_missing_password__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="sasl_password"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="user",
        )


def test__base_kafka_settings__sasl_plaintext_empty_password__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="sasl_password"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# SASL_SSL — requires sasl + ssl_cafile
# ---------------------------------------------------------------------------


def test__base_kafka_settings__sasl_ssl_with_all_required__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaSettings(
        bootstrap_servers="b:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="user",
        sasl_password="pass",  # type: ignore[arg-type]
        ssl_cafile="/ca.pem",
    )

    # Assert
    assert settings.security_protocol == "SASL_SSL"


def test__base_kafka_settings__sasl_ssl_missing_cafile__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="ssl_cafile"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # type: ignore[arg-type]
        )


# ---------------------------------------------------------------------------
# SSL — requires ssl_cafile
# ---------------------------------------------------------------------------


def test__base_kafka_settings__ssl_with_cafile__creates_successfully():
    # Arrange / Act
    settings = BaseKafkaSettings(
        bootstrap_servers="b:9092",
        security_protocol="SSL",
        ssl_cafile="/ca.pem",
    )

    # Assert
    assert settings.security_protocol == "SSL"
    assert settings.ssl_cafile == "/ca.pem"


def test__base_kafka_settings__ssl_missing_cafile__raises_validation_error():
    # Arrange / Act / Assert
    with pytest.raises(ValidationError, match="ssl_cafile"):
        BaseKafkaSettings(
            bootstrap_servers="b:9092",
            security_protocol="SSL",
        )


# ---------------------------------------------------------------------------
# Parametrize — SASL mechanisms
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
def test__base_kafka_settings__sasl_plaintext_all_mechanisms__accepted(mechanism: str):
    # Arrange / Act
    settings = BaseKafkaSettings(
        bootstrap_servers="b:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism=mechanism,  # type: ignore[arg-type]
        sasl_username="u",
        sasl_password="p",  # type: ignore[arg-type]
    )

    # Assert
    assert settings.sasl_mechanism == mechanism
