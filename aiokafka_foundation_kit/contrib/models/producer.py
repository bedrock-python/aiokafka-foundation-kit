"""Kafka producer settings."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from aiokafka_foundation_kit.config.kafka import KafkaAcks, KafkaCompressionType

from .kafka import BaseKafkaSettings


class KafkaAutoCreateMixin(BaseModel):
    """Mixin with topic auto-creation settings (kept separate from transport config).

    Only ``auto_create_topics`` is read by the library. The partition count and
    replication factor of a created topic come from the ``TopicConfig`` objects
    passed to :func:`~aiokafka_foundation_kit.producer.lifecycle.producer_lifecycle`
    or :func:`~aiokafka_foundation_kit.topics.management.ensure_topics_async`; the
    two ``default_*`` fields below are the caller's own defaults.
    """

    auto_create_topics: bool = Field(
        default=False,
        description="Auto-create topics on startup",
    )
    default_partitions: int = Field(
        default=3,
        ge=1,
        description="Caller-side default partition count; not read by the library",
    )
    default_replication_factor: int | None = Field(
        default=None,
        ge=1,
        description="Caller-side default replication factor; not read by the library",
    )


class BaseKafkaProducerSettings(BaseKafkaSettings, KafkaAutoCreateMixin):
    """Kafka producer configuration."""

    acks: KafkaAcks = Field(default="all", description="Acks: 0, 1, all")
    compression_type: KafkaCompressionType | None = Field(
        default="gzip",
        description="Compression: gzip, snappy, lz4, zstd, None",
    )
    enable_idempotence: bool = Field(default=True, description="Enable idempotent producer")
    max_batch_size: int = Field(default=16384, ge=0, description="Max batch size in bytes")
    linger_ms: int = Field(default=5, ge=0, description="Linger time in ms for batching")
    request_timeout_ms: int = Field(default=30000, ge=0, description="Request timeout in ms")

    @model_validator(mode="after")
    def _validate_auto_create(self) -> BaseKafkaProducerSettings:
        if not self.auto_create_topics:
            return self

        if self.default_replication_factor is None:
            msg = "Kafka default_replication_factor is required when auto_create_topics is enabled"
            raise ValueError(msg)

        return self
