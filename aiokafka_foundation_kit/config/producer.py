"""Kafka producer configuration protocol."""

from aiokafka_foundation_kit.config.kafka import KafkaAcks, KafkaCompressionType, KafkaSettingsProtocol


class ProducerSettingsProtocol(KafkaSettingsProtocol):
    """Protocol for Kafka producer settings."""

    acks: KafkaAcks
    compression_type: KafkaCompressionType | None
    enable_idempotence: bool
    max_batch_size: int
    linger_ms: int
    request_timeout_ms: int


class ProducerLifecycleSettingsProtocol(ProducerSettingsProtocol):
    """Producer settings that also carry topic auto-creation policy.

    Used by lifecycle/DI helpers that need to know whether to call
    :func:`ensure_topics_async` before starting the producer.
    """

    auto_create_topics: bool
