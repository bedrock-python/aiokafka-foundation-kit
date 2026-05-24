"""Topic management utilities."""

import logging
from collections.abc import Sequence

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from aiokafka_foundation_kit.config.kafka import KafkaSettingsProtocol
from aiokafka_foundation_kit.config.topic import TopicConfigProtocol
from aiokafka_foundation_kit.utils.config import build_kafka_common_config

logger = logging.getLogger(__name__)


def _to_new_topic(topic: TopicConfigProtocol) -> NewTopic:
    return NewTopic(
        name=topic.name,
        num_partitions=topic.num_partitions,
        replication_factor=topic.replication_factor,
        replica_assignments=topic.replica_assignment or {},
        topic_configs=topic.topic_configs or {},
    )


async def ensure_topics_async(
    topics: Sequence[TopicConfigProtocol],
    settings: KafkaSettingsProtocol,
) -> None:
    """Ensure Kafka topics exist, creating them if necessary.

    Topics are created one-by-one so that existing topics are reported
    individually (instead of treating the whole batch as "already exists" when
    any topic in the batch is new).

    Args:
        topics: Sequence of topic configurations to ensure.
        settings: Kafka settings for connection.

    Raises:
        Exception: If topic creation fails for reasons other than topic already existing.
    """
    if not topics:
        return

    admin_client = AIOKafkaAdminClient(**build_kafka_common_config(settings))
    try:
        await admin_client.start()
        for topic in topics:
            try:
                await admin_client.create_topics([_to_new_topic(topic)], validate_only=False)
                logger.info("Created topic: %s", topic.name)
            except TopicAlreadyExistsError:
                logger.debug("Topic already exists: %s", topic.name)
    finally:
        await admin_client.close()
