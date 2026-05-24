"""Kafka consumer settings."""

from pydantic import Field

from aiokafka_foundation_kit.config.kafka import KafkaOffsetReset

from .kafka import BaseKafkaSettings


class BaseKafkaConsumerSettings(BaseKafkaSettings):
    """Kafka consumer configuration.

    ``enable_auto_commit`` defaults to ``False`` because at-least-once consumers
    must commit offsets manually; flip it to ``True`` only for fire-and-forget
    or idempotent consumers.
    """

    group_id: str = Field(description="Consumer group ID")
    auto_offset_reset: KafkaOffsetReset = Field(default="earliest", description="Auto offset reset")
    enable_auto_commit: bool = Field(
        default=False,
        description="Enable broker-side auto-commit. Keep False for at-least-once delivery.",
    )
    session_timeout_ms: int = Field(default=30000, ge=0, description="Session timeout in ms")
    heartbeat_interval_ms: int = Field(default=3000, ge=0, description="Heartbeat interval in ms")
    max_poll_records: int = Field(default=500, ge=1, description="Max records per poll")
    max_poll_interval_ms: int = Field(default=300000, ge=1, description="Maximum poll interval in ms")
    fetch_max_wait_ms: int = Field(default=500, ge=0, description="Maximum wait time for fetch response")
    fetch_min_bytes: int = Field(default=1, ge=0, description="Minimum bytes server should return for fetch")
    fetch_max_bytes: int = Field(default=52428800, ge=1, description="Maximum bytes fetched per request")
