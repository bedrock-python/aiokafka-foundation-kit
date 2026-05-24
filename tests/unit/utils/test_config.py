"""Unit tests for aiokafka_foundation_kit.utils.config."""

from __future__ import annotations

import pytest

from aiokafka_foundation_kit.utils.config import build_kafka_common_config
from tests.unit.conftest import make_kafka_settings

# ---------------------------------------------------------------------------
# PLAINTEXT — always-present keys
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__plaintext__returns_base_keys(plaintext_settings):
    # Arrange
    # plaintext_settings fixture: PLAINTEXT, no client_id

    # Act
    result = build_kafka_common_config(plaintext_settings)

    # Assert
    assert result["bootstrap_servers"] == "localhost:9092"
    assert result["security_protocol"] == "PLAINTEXT"
    assert result["metadata_max_age_ms"] == 300000


def test__build_kafka_common_config__plaintext__does_not_include_client_id_when_none(plaintext_settings):
    # Arrange
    plaintext_settings.client_id = None

    # Act
    result = build_kafka_common_config(plaintext_settings)

    # Assert
    assert "client_id" not in result


def test__build_kafka_common_config__with_client_id__includes_client_id():
    # Arrange
    settings = make_kafka_settings(client_id="my-service")

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["client_id"] == "my-service"


def test__build_kafka_common_config__plaintext__does_not_include_sasl_keys(plaintext_settings):
    # Arrange / Act
    result = build_kafka_common_config(plaintext_settings)

    # Assert
    assert "sasl_mechanism" not in result
    assert "sasl_plain_username" not in result
    assert "sasl_plain_password" not in result


def test__build_kafka_common_config__plaintext__does_not_include_ssl_keys(plaintext_settings):
    # Arrange / Act
    result = build_kafka_common_config(plaintext_settings)

    # Assert
    assert "ssl_cafile" not in result
    assert "ssl_certfile" not in result
    assert "ssl_keyfile" not in result
    assert "ssl_check_hostname" not in result


# ---------------------------------------------------------------------------
# SASL_PLAINTEXT
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__sasl_plaintext__includes_sasl_keys():
    # Arrange
    settings = make_kafka_settings(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username="user",
    )
    settings.get_sasl_password.return_value = "secret"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["sasl_mechanism"] == "PLAIN"
    assert result["sasl_plain_username"] == "user"
    assert result["sasl_plain_password"] == "secret"


def test__build_kafka_common_config__sasl_plaintext__does_not_include_ssl_keys():
    # Arrange
    settings = make_kafka_settings(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username="user",
    )
    settings.get_sasl_password.return_value = "secret"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "ssl_cafile" not in result
    assert "ssl_check_hostname" not in result


# ---------------------------------------------------------------------------
# SASL_SSL
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__sasl_ssl__includes_both_sasl_and_ssl_keys():
    # Arrange
    settings = make_kafka_settings(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="user",
        ssl_cafile="/ca.pem",
        ssl_certfile="/cert.pem",
        ssl_keyfile="/key.pem",
        ssl_check_hostname=True,
    )
    settings.get_sasl_password.return_value = "s3cr3t"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["sasl_mechanism"] == "SCRAM-SHA-256"
    assert result["sasl_plain_username"] == "user"
    assert result["sasl_plain_password"] == "s3cr3t"
    assert result["ssl_cafile"] == "/ca.pem"
    assert result["ssl_certfile"] == "/cert.pem"
    assert result["ssl_keyfile"] == "/key.pem"
    assert result["ssl_check_hostname"] is True


def test__build_kafka_common_config__sasl_ssl__omits_ssl_files_when_none():
    # Arrange — no certfile/keyfile; only cafile
    settings = make_kafka_settings(
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_username="user",
        ssl_cafile="/ca.pem",
        ssl_certfile=None,
        ssl_keyfile=None,
        ssl_check_hostname=False,
    )
    settings.get_sasl_password.return_value = "pw"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["ssl_cafile"] == "/ca.pem"
    assert "ssl_certfile" not in result
    assert "ssl_keyfile" not in result
    assert result["ssl_check_hostname"] is False


# ---------------------------------------------------------------------------
# SSL (no SASL)
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__ssl__includes_ssl_keys_no_sasl():
    # Arrange
    settings = make_kafka_settings(
        security_protocol="SSL",
        ssl_cafile="/ca.pem",
        ssl_certfile="/cert.pem",
        ssl_keyfile="/key.pem",
        ssl_check_hostname=True,
    )

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["ssl_cafile"] == "/ca.pem"
    assert result["ssl_certfile"] == "/cert.pem"
    assert result["ssl_keyfile"] == "/key.pem"
    assert result["ssl_check_hostname"] is True
    assert "sasl_mechanism" not in result


def test__build_kafka_common_config__ssl__omits_ssl_files_when_falsy():
    # Arrange — no certfile/keyfile
    settings = make_kafka_settings(
        security_protocol="SSL",
        ssl_cafile="/ca.pem",
        ssl_certfile=None,
        ssl_keyfile=None,
        ssl_check_hostname=True,
    )

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "ssl_certfile" not in result
    assert "ssl_keyfile" not in result


# ---------------------------------------------------------------------------
# Parametrized — SASL protocols both get sasl keys
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("protocol", ["SASL_PLAINTEXT", "SASL_SSL"])
def test__build_kafka_common_config__sasl_protocols__always_set_sasl_keys(protocol: str):
    # Arrange
    settings = make_kafka_settings(
        security_protocol=protocol,
        sasl_mechanism="PLAIN",
        sasl_username="u",
        ssl_cafile="/ca.pem" if protocol == "SASL_SSL" else None,
        ssl_check_hostname=True,
    )
    settings.get_sasl_password.return_value = "p"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "sasl_mechanism" in result
    assert "sasl_plain_username" in result
    assert "sasl_plain_password" in result


@pytest.mark.parametrize("protocol", ["SSL", "SASL_SSL"])
def test__build_kafka_common_config__ssl_protocols__always_set_ssl_check_hostname(protocol: str):
    # Arrange
    settings = make_kafka_settings(
        security_protocol=protocol,
        sasl_mechanism="PLAIN" if protocol == "SASL_SSL" else None,
        sasl_username="u" if protocol == "SASL_SSL" else None,
        ssl_cafile="/ca.pem",
        ssl_check_hostname=True,
    )
    settings.get_sasl_password.return_value = "p" if protocol == "SASL_SSL" else None

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "ssl_check_hostname" in result
