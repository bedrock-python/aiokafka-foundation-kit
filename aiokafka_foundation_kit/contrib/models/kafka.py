"""Base Kafka configuration."""

from __future__ import annotations

from pydantic import BaseModel, Field, SecretStr, model_validator

from aiokafka_foundation_kit.config.kafka import (
    KafkaSaslMechanism,
    KafkaSecurityProtocol,
)


class KafkaConnectionMixin(BaseModel):
    """Kafka transport connection parameters."""

    bootstrap_servers: str = Field(description="Kafka bootstrap servers (comma-separated)")
    client_id: str | None = Field(default=None, description="Client ID for Kafka")
    metadata_max_age_ms: int = Field(default=300000, ge=0, description="Metadata max age in ms")


class KafkaSaslMixin(BaseModel):
    """SASL credentials. Required when ``security_protocol`` selects SASL_*."""

    sasl_mechanism: KafkaSaslMechanism | None = Field(
        default=None,
        description="SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
    )
    sasl_username: str | None = Field(default=None, description="SASL username")
    sasl_password: SecretStr | None = Field(default=None, description="SASL password")

    def get_sasl_password(self) -> str | None:
        """Plaintext SASL password for Kafka client config. Do NOT log."""
        if self.sasl_password is None:
            return None
        secret: str = self.sasl_password.get_secret_value()
        return secret


class KafkaSslMixin(BaseModel):
    """TLS material. Required when ``security_protocol`` selects SSL/SASL_SSL."""

    ssl_cafile: str | None = Field(default=None, description="Path to CA certificate")
    ssl_certfile: str | None = Field(default=None, description="Path to client certificate")
    ssl_keyfile: str | None = Field(default=None, description="Path to client key")
    ssl_check_hostname: bool = Field(default=True, description="Verify hostname")


class BaseKafkaSettings(KafkaConnectionMixin, KafkaSaslMixin, KafkaSslMixin):
    """Base Kafka configuration shared between producer and consumer."""

    security_protocol: KafkaSecurityProtocol = Field(
        default="PLAINTEXT",
        description="Security protocol: PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL",
    )

    @model_validator(mode="after")
    def _validate_sasl(self) -> BaseKafkaSettings:
        if self.security_protocol not in {"SASL_PLAINTEXT", "SASL_SSL"}:
            return self

        if not self.sasl_mechanism:
            msg = f"Kafka sasl_mechanism is required for {self.security_protocol}"
            raise ValueError(msg)
        if not self.sasl_username:
            msg = f"Kafka sasl_username is required for {self.security_protocol}"
            raise ValueError(msg)
        if self.sasl_password is None or not self.sasl_password.get_secret_value():
            msg = f"Kafka sasl_password is required for {self.security_protocol}"
            raise ValueError(msg)

        return self

    @model_validator(mode="after")
    def _validate_ssl(self) -> BaseKafkaSettings:
        if self.security_protocol not in {"SSL", "SASL_SSL"}:
            return self

        if not self.ssl_cafile:
            msg = f"Kafka ssl_cafile is required for {self.security_protocol}"
            raise ValueError(msg)

        return self
