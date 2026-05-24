"""Kafka consumer."""

from aiokafka_foundation_kit.consumer.client import create_async_kafka_consumer
from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle

__all__ = [
    "consumer_lifecycle",
    "create_async_kafka_consumer",
]
