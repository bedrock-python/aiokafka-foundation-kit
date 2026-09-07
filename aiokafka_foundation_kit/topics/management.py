"""Topic management utilities."""

import logging
from collections.abc import Sequence

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code
from aiokafka.protocol.api import Response

from aiokafka_foundation_kit.config.kafka import KafkaSettingsProtocol
from aiokafka_foundation_kit.config.topic import TopicConfigProtocol
from aiokafka_foundation_kit.utils.config import build_kafka_common_config

logger = logging.getLogger(__name__)

_ASSIGNMENT_DECIDES = -1
_NO_ERROR = 0


def _reject_contradicting_counts(topic: TopicConfigProtocol, replica_assignment: dict[int, list[int]]) -> None:
    """Refuse a config whose counts describe a different topic than its assignment.

    The broker follows the assignment, so a stale ``num_partitions`` next to it
    would silently create a topic of another shape. ``-1`` on either count is
    aiokafka's way of saying "the assignment decides" and never contradicts it.
    """
    partitions = len(replica_assignment)
    if topic.num_partitions not in (_ASSIGNMENT_DECIDES, partitions):
        raise ValueError(
            f"topic {topic.name!r}: replica_assignment describes {partitions} partition(s), "
            f"but num_partitions is {topic.num_partitions}; pass -1 to let the assignment decide"
        )

    factors = {len(replicas) for replicas in replica_assignment.values()}
    if len(factors) == 1 and topic.replication_factor not in (_ASSIGNMENT_DECIDES, *factors):
        raise ValueError(
            f"topic {topic.name!r}: replica_assignment gives {next(iter(factors))} replica(s) per partition, "
            f"but replication_factor is {topic.replication_factor}; pass -1 to let the assignment decide"
        )


def _to_new_topic(topic: TopicConfigProtocol) -> NewTopic:
    """Map one topic config onto the ``NewTopic`` aiokafka accepts.

    ``NewTopic`` takes either a partition count and a replication factor or an
    explicit replica assignment, never both: an assignment has to arrive with
    ``num_partitions`` and ``replication_factor`` set to ``-1``, and a plain
    config has to arrive with no assignment at all -- ``{}`` counts as one and
    is rejected.
    """
    replica_assignment = topic.replica_assignment or None
    num_partitions = topic.num_partitions
    replication_factor = topic.replication_factor

    if replica_assignment is not None:
        _reject_contradicting_counts(topic, replica_assignment)
        num_partitions = replication_factor = _ASSIGNMENT_DECIDES

    return NewTopic(
        name=topic.name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        replica_assignments=replica_assignment,
        topic_configs=topic.topic_configs or {},
    )


def _raise_for_topic_errors(response: Response) -> None:
    """Raise whatever the broker said about the topics in a create request.

    ``AIOKafkaAdminClient.create_topics`` hands the reply back untouched, and a
    refused topic arrives as an error code inside an otherwise successful
    response -- so a caller that does not read ``topic_errors`` cannot tell a
    created topic from a rejected one.
    """
    for topic_error in response.topic_errors:
        name, error_code = topic_error[0], topic_error[1]
        if error_code == _NO_ERROR:
            continue

        message = topic_error[2] if len(topic_error) > 2 else None
        raise for_code(error_code)(message or f"Broker refused topic {name!r} with error code {error_code}")


async def ensure_topics_async(
    topics: Sequence[TopicConfigProtocol],
    settings: KafkaSettingsProtocol,
) -> None:
    """Ensure Kafka topics exist, creating them if necessary.

    Topics are created one-by-one so that existing topics are reported
    individually (instead of treating the whole batch as "already exists" when
    any topic in the batch is new).

    A topic carrying a ``replica_assignment`` is created from that assignment:
    it decides both the partition count and the replication factor, and the
    two count fields may only repeat what it says or be ``-1``.

    Args:
        topics: Sequence of topic configurations to ensure.
        settings: Kafka settings for connection.

    Raises:
        ValueError: If a topic's ``num_partitions`` or ``replication_factor``
            contradicts the ``replica_assignment`` it carries.
        KafkaError: Whatever the broker answered for a topic it refused --
            ``InvalidReplicationFactorError`` for a replication factor above
            the broker count, ``InvalidTopicError`` for an unusable name, and
            so on. The remaining topics in the sequence are not attempted.
    """
    if not topics:
        return

    admin_client = AIOKafkaAdminClient(**build_kafka_common_config(settings))
    try:
        await admin_client.start()
        for topic in topics:
            try:
                response = await admin_client.create_topics([_to_new_topic(topic)], validate_only=False)
                _raise_for_topic_errors(response)
                logger.info("Created topic: %s", topic.name)
            except TopicAlreadyExistsError:
                logger.debug("Topic already exists: %s", topic.name)
    finally:
        await admin_client.close()
