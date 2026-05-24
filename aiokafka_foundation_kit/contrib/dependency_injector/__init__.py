"""Dependency-injector containers for Kafka."""

from __future__ import annotations

from .consumer import KafkaConsumerContainer
from .producer import KafkaProducerContainer

__all__ = [
    "KafkaConsumerContainer",
    "KafkaProducerContainer",
]
