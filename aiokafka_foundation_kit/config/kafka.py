"""Kafka configuration protocol and type aliases."""

from typing import Literal, Protocol

KafkaSecurityProtocol = Literal["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"]
KafkaSaslMechanism = Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
KafkaAcks = Literal["0", "1", "all"]
KafkaCompressionType = Literal["gzip", "snappy", "lz4", "zstd"]
KafkaOffsetReset = Literal["earliest", "latest"]


class KafkaConnectionSettingsProtocol(Protocol):
    """Protocol for core Kafka connection fields only."""

    bootstrap_servers: str
    client_id: str | None
    security_protocol: KafkaSecurityProtocol
    metadata_max_age_ms: int

    def get_sasl_password(self) -> str | None: ...


class KafkaSaslSettingsProtocol(KafkaConnectionSettingsProtocol, Protocol):
    """Protocol for SASL credentials. Relevant when ``security_protocol`` is ``SASL_*``."""

    sasl_mechanism: KafkaSaslMechanism | None
    sasl_username: str | None


class KafkaSslSettingsProtocol(KafkaConnectionSettingsProtocol, Protocol):
    """Protocol for TLS material. Relevant when ``security_protocol`` is ``SSL`` or ``SASL_SSL``."""

    ssl_cafile: str | None
    ssl_certfile: str | None
    ssl_keyfile: str | None
    ssl_check_hostname: bool


class KafkaSettingsProtocol(KafkaSaslSettingsProtocol, KafkaSslSettingsProtocol, Protocol):
    """Protocol for base Kafka settings (connection + SASL + SSL)."""
