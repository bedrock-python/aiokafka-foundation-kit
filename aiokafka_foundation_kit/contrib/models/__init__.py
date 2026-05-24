"""Pydantic models for Kafka configuration."""

from __future__ import annotations

from aiokafka_foundation_kit.config.kafka import (
    KafkaAcks,
    KafkaCompressionType,
    KafkaOffsetReset,
    KafkaSaslMechanism,
    KafkaSecurityProtocol,
)

from .consumer import BaseKafkaConsumerSettings
from .infra import BaseKafkaInfraSettings, KafkaTopicSettings, normalize_kafka_topic_prefix_value
from .kafka import BaseKafkaSettings, KafkaConnectionMixin, KafkaSaslMixin, KafkaSslMixin
from .producer import BaseKafkaProducerSettings, KafkaAutoCreateMixin

__all__ = [
    "BaseKafkaConsumerSettings",
    "BaseKafkaInfraSettings",
    "BaseKafkaProducerSettings",
    "BaseKafkaSettings",
    "KafkaAcks",
    "KafkaAutoCreateMixin",
    "KafkaCompressionType",
    "KafkaConnectionMixin",
    "KafkaOffsetReset",
    "KafkaSaslMechanism",
    "KafkaSaslMixin",
    "KafkaSecurityProtocol",
    "KafkaSslMixin",
    "KafkaTopicSettings",
    "normalize_kafka_topic_prefix_value",
]
