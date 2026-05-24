"""Dishka dependency injection providers for Kafka."""

from __future__ import annotations

from .consumer import AsyncKafkaConsumerProvider
from .infra import (
    KafkaConsumerInfraSettingsProtocol,
    KafkaInfraBaseSettingsProtocol,
    KafkaInfraProvider,
    KafkaProducerInfraSettingsProtocol,
    KafkaTopicSettingsProtocol,
)
from .producer import AsyncKafkaProducerProvider

__all__ = [
    "AsyncKafkaConsumerProvider",
    "AsyncKafkaProducerProvider",
    "KafkaConsumerInfraSettingsProtocol",
    "KafkaInfraBaseSettingsProtocol",
    "KafkaInfraProvider",
    "KafkaProducerInfraSettingsProtocol",
    "KafkaTopicSettingsProtocol",
]
