"""Integration tests for aiokafka_foundation_kit.topics.management against a real broker."""

from __future__ import annotations

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import InvalidReplicationFactorError

from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings
from aiokafka_foundation_kit.producer.lifecycle import producer_lifecycle
from aiokafka_foundation_kit.topics.config import TopicConfig
from aiokafka_foundation_kit.topics.management import ensure_topics_async


async def _exists(admin_client: AIOKafkaAdminClient, name: str) -> bool:
    return name in await admin_client.list_topics()


async def _partitions(admin_client: AIOKafkaAdminClient, name: str) -> list[dict]:
    described = await admin_client.describe_topics([name])
    return sorted(described[0]["partitions"], key=lambda partition: partition["partition"])


async def _broker_id(admin_client: AIOKafkaAdminClient) -> int:
    cluster = await admin_client.describe_cluster()
    return int(cluster["brokers"][0]["node_id"])


# ---------------------------------------------------------------------------
# ensure_topics_async — a plain TopicConfig reaches the broker
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__plain_topic_config__creates_the_topic(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    topics = [TopicConfig(name=topic_name, num_partitions=3, replication_factor=1)]

    # Act
    await ensure_topics_async(topics, kafka_settings)

    # Assert
    assert await _exists(admin_client, topic_name)
    assert len(await _partitions(admin_client, topic_name)) == 3


async def test__ensure_topics_async__topic_configs__reach_the_broker(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    topics = [
        TopicConfig(
            name=topic_name,
            num_partitions=1,
            replication_factor=1,
            topic_configs={"retention.ms": "604800000"},
        )
    ]

    # Act
    await ensure_topics_async(topics, kafka_settings)

    # Assert — a described resource is (error_code, error_message, type, name, entries)
    resource = ConfigResource(ConfigResourceType.TOPIC, topic_name, configs={"retention.ms": None})
    described = await admin_client.describe_configs([resource])
    entries = {entry[0]: entry[1] for entry in described[0].resources[0][4]}
    assert entries["retention.ms"] == "604800000"


async def test__ensure_topics_async__several_topics__creates_every_one(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    names = [f"{topic_name}-{index}" for index in range(3)]
    topics = [TopicConfig(name=name, num_partitions=1, replication_factor=1) for name in names]

    # Act
    await ensure_topics_async(topics, kafka_settings)

    # Assert
    existing = await admin_client.list_topics()
    assert set(names) <= set(existing)


# ---------------------------------------------------------------------------
# ensure_topics_async — an existing topic is success, other broker errors are not
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__called_twice__second_call_is_a_no_op(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange — the second run asks for a different shape of the same topic
    await ensure_topics_async([TopicConfig(name=topic_name, num_partitions=3, replication_factor=1)], kafka_settings)

    # Act — TopicAlreadyExistsError is swallowed, so this does not raise
    await ensure_topics_async([TopicConfig(name=topic_name, num_partitions=6, replication_factor=1)], kafka_settings)

    # Assert — and the existing topic is left exactly as it was
    assert len(await _partitions(admin_client, topic_name)) == 3


async def test__ensure_topics_async__replication_factor_above_broker_count__propagates(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange — one broker in the container, three replicas asked for
    topics = [TopicConfig(name=topic_name, num_partitions=1, replication_factor=3)]

    # Act / Assert
    with pytest.raises(InvalidReplicationFactorError):
        await ensure_topics_async(topics, kafka_settings)

    assert not await _exists(admin_client, topic_name)


async def test__ensure_topics_async__failing_topic__aborts_the_rest_of_the_sequence(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    topics = [
        TopicConfig(name=f"{topic_name}-first", num_partitions=1, replication_factor=1),
        TopicConfig(name=f"{topic_name}-bad", num_partitions=1, replication_factor=3),
        TopicConfig(name=f"{topic_name}-last", num_partitions=1, replication_factor=1),
    ]

    # Act / Assert
    with pytest.raises(InvalidReplicationFactorError):
        await ensure_topics_async(topics, kafka_settings)

    existing = await admin_client.list_topics()
    assert f"{topic_name}-first" in existing
    assert f"{topic_name}-last" not in existing


# ---------------------------------------------------------------------------
# ensure_topics_async — an explicit replica assignment decides the shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "num_partitions,replication_factor",
    [
        (3, 1),
        (-1, -1),
    ],
)
async def test__ensure_topics_async__explicit_replica_assignment__creates_the_assigned_partitions(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
    num_partitions: int,
    replication_factor: int,
):
    # Arrange — the only broker in the container takes every partition
    broker = await _broker_id(admin_client)
    topics = [
        TopicConfig(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            replica_assignment={0: [broker], 1: [broker], 2: [broker]},
        )
    ]

    # Act
    await ensure_topics_async(topics, kafka_settings)

    # Assert
    partitions = await _partitions(admin_client, topic_name)
    assert len(partitions) == 3
    assert [partition["replicas"] for partition in partitions] == [[broker], [broker], [broker]]


async def test__ensure_topics_async__counts_contradict_the_assignment__creates_nothing(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    broker = await _broker_id(admin_client)
    topics = [
        TopicConfig(
            name=topic_name,
            num_partitions=6,
            replication_factor=1,
            replica_assignment={0: [broker], 1: [broker], 2: [broker]},
        )
    ]

    # Act / Assert
    with pytest.raises(ValueError, match="describes 3 partition"):
        await ensure_topics_async(topics, kafka_settings)

    assert not await _exists(admin_client, topic_name)


# ---------------------------------------------------------------------------
# producer_lifecycle — the documented auto-creation path
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__auto_create_topics__creates_the_topic_and_sends(
    kafka_settings: BaseKafkaProducerSettings,
    admin_client: AIOKafkaAdminClient,
    topic_name: str,
):
    # Arrange
    topics = [TopicConfig(name=topic_name, num_partitions=6, replication_factor=1)]

    # Act
    async with producer_lifecycle(kafka_settings, topics=topics, auto_create_topics=True) as producer:
        record = await producer.send_and_wait(topic_name, {"id": 1})

    # Assert
    assert record.topic == topic_name
    assert len(await _partitions(admin_client, topic_name)) == 6
