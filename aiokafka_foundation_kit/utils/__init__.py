"""Common utilities."""

from aiokafka_foundation_kit.utils.config import build_kafka_common_config
from aiokafka_foundation_kit.utils.json import dumps_bytes, loads_bytes
from aiokafka_foundation_kit.utils.lifecycle import managed_kafka_client

__all__ = [
    "build_kafka_common_config",
    "dumps_bytes",
    "loads_bytes",
    "managed_kafka_client",
]
