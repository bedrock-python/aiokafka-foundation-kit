"""Kafka health check utilities."""

import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from aiokafka_foundation_kit.config.kafka import KafkaSettingsProtocol
from aiokafka_foundation_kit.utils.config import build_kafka_common_config

logger = logging.getLogger(__name__)


async def check_kafka_health_async(settings: KafkaSettingsProtocol, timeout_seconds: float = 5.0) -> bool:
    """Check Kafka cluster health by attempting to connect.

    Args:
        settings: Kafka settings for connection.
        timeout_seconds: Connection timeout in seconds.

    Returns:
        True if Kafka is healthy, False otherwise.
    """
    config = build_kafka_common_config(settings)
    config["request_timeout_ms"] = int(timeout_seconds * 1000)

    producer = AIOKafkaProducer(**config)

    try:
        await producer.start()
        await producer.stop()
    except (TimeoutError, KafkaError, OSError):
        logger.exception("Kafka health check failed")
        return False

    logger.debug("Kafka health check passed")
    return True
