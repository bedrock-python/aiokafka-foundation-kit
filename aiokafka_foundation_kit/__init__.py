"""Async Kafka foundation library — factories, settings, DI providers and OpenTelemetry on top of aiokafka."""

from aiokafka_foundation_kit.__version__ import __version__
from aiokafka_foundation_kit.config import (
    ConsumerSettingsProtocol,
    KafkaSettingsProtocol,
    ProducerSettingsProtocol,
    TopicConfigProtocol,
)
from aiokafka_foundation_kit.consumer import consumer_lifecycle, create_async_kafka_consumer
from aiokafka_foundation_kit.health import check_kafka_health_async
from aiokafka_foundation_kit.producer import create_async_kafka_producer, producer_lifecycle
from aiokafka_foundation_kit.topics import TopicConfig, ensure_topics_async
from aiokafka_foundation_kit.utils import build_kafka_common_config

__all__ = [
    "ConsumerSettingsProtocol",
    "KafkaSettingsProtocol",
    "ProducerSettingsProtocol",
    "TopicConfig",
    "TopicConfigProtocol",
    "__version__",
    "build_kafka_common_config",
    "check_kafka_health_async",
    "consumer_lifecycle",
    "create_async_kafka_consumer",
    "create_async_kafka_producer",
    "ensure_topics_async",
    "producer_lifecycle",
]

# Note: Optional contrib modules are available via explicit imports:
# - from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings
# - from aiokafka_foundation_kit.contrib.di import AsyncKafkaProducerProvider
# - from aiokafka_foundation_kit.contrib.dependency_injector import KafkaProducerContainer
# - from aiokafka_foundation_kit.contrib.telemetry import instrument_aiokafka
