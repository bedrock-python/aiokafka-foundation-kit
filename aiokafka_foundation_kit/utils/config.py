"""Kafka configuration utilities."""

import ssl
from typing import Any

from aiokafka.helpers import create_ssl_context

from aiokafka_foundation_kit.config.kafka import KafkaSettingsProtocol, KafkaSslSettingsProtocol


def _build_ssl_context(settings: KafkaSslSettingsProtocol) -> ssl.SSLContext:
    """Build the ``ssl.SSLContext`` aiokafka expects for SSL/SASL_SSL.

    aiokafka takes a prepared context, not the individual file paths, so the
    ``ssl_*`` fields are loaded here. With no ``ssl_cafile`` the system trust
    store is used.
    """
    context: ssl.SSLContext = create_ssl_context(
        cafile=settings.ssl_cafile,
        certfile=settings.ssl_certfile,
        keyfile=settings.ssl_keyfile,
    )
    if not settings.ssl_check_hostname:
        context.check_hostname = False

    return context


def build_kafka_common_config(settings: KafkaSettingsProtocol) -> dict[str, Any]:
    """Build common Kafka client configuration from settings.

    For ``SSL`` and ``SASL_SSL`` the ``ssl_*`` settings are loaded into an
    ``ssl.SSLContext`` and passed as ``ssl_context``, which is the only TLS
    parameter aiokafka accepts.

    Args:
        settings: Kafka settings object conforming to KafkaSettingsProtocol.

    Returns:
        Dictionary of Kafka client configuration parameters.

    Raises:
        OSError: If a configured certificate or key file cannot be read.
        ssl.SSLError: If a configured certificate or key file cannot be parsed.
    """
    config: dict[str, Any] = {
        "bootstrap_servers": settings.bootstrap_servers,
        "security_protocol": settings.security_protocol,
        "metadata_max_age_ms": settings.metadata_max_age_ms,
    }

    if settings.client_id:
        config["client_id"] = settings.client_id

    if settings.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        config["sasl_mechanism"] = settings.sasl_mechanism
        config["sasl_plain_username"] = settings.sasl_username
        config["sasl_plain_password"] = settings.get_sasl_password()

    if settings.security_protocol in ("SSL", "SASL_SSL"):
        config["ssl_context"] = _build_ssl_context(settings)

    return config
