"""Kafka producer."""

from aiokafka_foundation_kit.producer.client import create_async_kafka_producer
from aiokafka_foundation_kit.producer.lifecycle import producer_lifecycle

__all__ = [
    "create_async_kafka_producer",
    "producer_lifecycle",
]
