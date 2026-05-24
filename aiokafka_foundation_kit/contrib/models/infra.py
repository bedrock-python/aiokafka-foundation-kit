"""Kafka infrastructure settings."""

from pydantic import BaseModel, Field, field_validator


def normalize_kafka_topic_prefix_value(value: object) -> str | None:
    """Return a real prefix or ``None`` when unset.

    Vault/env sometimes stringify JSON ``null`` as the literal strings ``"None"`` or
    ``"null"``, which would otherwise produce physical topic names like ``None.auth.*``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in ("none", "null"):
            return None
        return s
    return None


class KafkaTopicSettings(BaseModel):
    """Physical parameters for a specific Kafka topic."""

    num_partitions: int = Field(default=3, ge=1, description="Number of partitions for the topic")
    replication_factor: int = Field(default=3, ge=1, description="Replication factor for the topic")
    topic_configs: dict[str, str] | None = Field(default=None, description="Topic-specific configuration overrides")


class BaseKafkaInfraSettings(BaseModel):
    """Shared Kafka infrastructure settings."""

    topic_prefix: str | None = Field(default=None, description="Global prefix for all service topics")
    topic_catalog: dict[str, KafkaTopicSettings] | None = Field(
        default=None,
        description="Map of logical topic names to physical settings for topic creation",
    )
    consumer_subscriptions: list[str] | None = Field(
        default=None,
        description="Logical topic names to subscribe for inbox consumers",
    )

    @field_validator("topic_prefix", mode="before")
    @classmethod
    def _coerce_topic_prefix(cls, value: object) -> str | None:
        return normalize_kafka_topic_prefix_value(value)
