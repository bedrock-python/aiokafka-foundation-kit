"""Configuration protocols for aiokafka-foundation-kit.

These protocols define the interfaces that settings objects must implement,
allowing the library to work with any settings implementation (pydantic models,
dataclasses, or custom classes).
"""

from __future__ import annotations

from .consumer import ConsumerSettingsProtocol
from .kafka import (
    KafkaConnectionSettingsProtocol,
    KafkaSaslSettingsProtocol,
    KafkaSettingsProtocol,
    KafkaSslSettingsProtocol,
)
from .producer import ProducerSettingsProtocol
from .topic import TopicConfigProtocol

__all__ = [
    "ConsumerSettingsProtocol",
    "KafkaConnectionSettingsProtocol",
    "KafkaSaslSettingsProtocol",
    "KafkaSettingsProtocol",
    "KafkaSslSettingsProtocol",
    "ProducerSettingsProtocol",
    "TopicConfigProtocol",
]
