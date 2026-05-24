"""Kafka consumer configuration protocol."""

from aiokafka_foundation_kit.config.kafka import KafkaOffsetReset, KafkaSettingsProtocol


class ConsumerSettingsProtocol(KafkaSettingsProtocol):
    """Protocol for Kafka consumer settings."""

    group_id: str
    auto_offset_reset: KafkaOffsetReset
    enable_auto_commit: bool
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_records: int
    max_poll_interval_ms: int
    fetch_max_wait_ms: int
    fetch_min_bytes: int
    fetch_max_bytes: int
