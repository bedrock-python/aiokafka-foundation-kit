"""Unit tests for aiokafka_foundation_kit.utils.config."""

from __future__ import annotations

import ssl
from unittest.mock import patch

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
    assert "ssl_context" not in result


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
    assert "ssl_context" not in result


# ---------------------------------------------------------------------------
# SASL_SSL
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__sasl_ssl__includes_sasl_keys_and_ssl_context():
    # Arrange — no cafile, so the system trust store is used and no file is read
    settings = make_kafka_settings(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_username="user",
        ssl_cafile=None,
    )
    settings.get_sasl_password.return_value = "s3cr3t"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["sasl_mechanism"] == "SCRAM-SHA-256"
    assert result["sasl_plain_username"] == "user"
    assert result["sasl_plain_password"] == "s3cr3t"
    assert isinstance(result["ssl_context"], ssl.SSLContext)


# ---------------------------------------------------------------------------
# SSL (no SASL)
# ---------------------------------------------------------------------------


def test__build_kafka_common_config__ssl__returns_ssl_context_and_no_sasl_keys():
    # Arrange
    settings = make_kafka_settings(security_protocol="SSL", ssl_cafile=None)

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert isinstance(result["ssl_context"], ssl.SSLContext)
    assert "sasl_mechanism" not in result


def test__build_kafka_common_config__ssl__does_not_emit_raw_ssl_file_keys():
    # Arrange — aiokafka accepts none of these; only ssl_context
    settings = make_kafka_settings(security_protocol="SSL", ssl_cafile=None)

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "ssl_cafile" not in result
    assert "ssl_certfile" not in result
    assert "ssl_keyfile" not in result
    assert "ssl_check_hostname" not in result


def test__build_kafka_common_config__ssl__forwards_cert_paths_to_context_builder():
    # Arrange
    settings = make_kafka_settings(
        security_protocol="SSL",
        ssl_cafile="/ca.pem",
        ssl_certfile="/cert.pem",
        ssl_keyfile="/key.pem",
    )

    # Act
    with patch("aiokafka_foundation_kit.utils.config.create_ssl_context") as mock_create:
        result = build_kafka_common_config(settings)

    # Assert
    mock_create.assert_called_once_with(cafile="/ca.pem", certfile="/cert.pem", keyfile="/key.pem")
    assert result["ssl_context"] is mock_create.return_value


def test__build_kafka_common_config__ssl_check_hostname_true__context_checks_hostname():
    # Arrange
    settings = make_kafka_settings(security_protocol="SSL", ssl_cafile=None, ssl_check_hostname=True)

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["ssl_context"].check_hostname is True


def test__build_kafka_common_config__ssl_check_hostname_false__context_skips_hostname_check():
    # Arrange
    settings = make_kafka_settings(security_protocol="SSL", ssl_cafile=None, ssl_check_hostname=False)

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert result["ssl_context"].check_hostname is False


def test__build_kafka_common_config__ssl__missing_cafile__raises_os_error():
    # Arrange
    settings = make_kafka_settings(security_protocol="SSL", ssl_cafile="/nonexistent/ca.pem")

    # Act / Assert
    with pytest.raises(FileNotFoundError):
        build_kafka_common_config(settings)


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
        ssl_cafile=None,
    )
    settings.get_sasl_password.return_value = "p"

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert "sasl_mechanism" in result
    assert "sasl_plain_username" in result
    assert "sasl_plain_password" in result


@pytest.mark.parametrize("protocol", ["SSL", "SASL_SSL"])
def test__build_kafka_common_config__ssl_protocols__always_set_ssl_context(protocol: str):
    # Arrange
    settings = make_kafka_settings(
        security_protocol=protocol,
        sasl_mechanism="PLAIN" if protocol == "SASL_SSL" else None,
        sasl_username="u" if protocol == "SASL_SSL" else None,
        ssl_cafile=None,
    )
    settings.get_sasl_password.return_value = "p" if protocol == "SASL_SSL" else None

    # Act
    result = build_kafka_common_config(settings)

    # Assert
    assert isinstance(result["ssl_context"], ssl.SSLContext)
