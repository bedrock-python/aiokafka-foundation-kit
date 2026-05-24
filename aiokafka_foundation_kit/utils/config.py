"""Kafka configuration utilities."""

from typing import Any

from aiokafka_foundation_kit.config.kafka import KafkaSettingsProtocol


def build_kafka_common_config(settings: KafkaSettingsProtocol) -> dict[str, Any]:
    """Build common Kafka client configuration from settings.

    Args:
        settings: Kafka settings object conforming to KafkaSettingsProtocol.

    Returns:
        Dictionary of Kafka client configuration parameters.
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
        if settings.ssl_cafile:
            config["ssl_cafile"] = settings.ssl_cafile
        if settings.ssl_certfile:
            config["ssl_certfile"] = settings.ssl_certfile
        if settings.ssl_keyfile:
            config["ssl_keyfile"] = settings.ssl_keyfile
        config["ssl_check_hostname"] = settings.ssl_check_hostname

    return config
