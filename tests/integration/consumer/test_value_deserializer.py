"""Integration tests for the value_deserializer of consumer_lifecycle against a real broker."""

from __future__ import annotations

from aiokafka import AIOKafkaConsumer
from testcontainers.kafka import KafkaContainer

from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaConsumerSettings, BaseKafkaProducerSettings
from aiokafka_foundation_kit.producer.lifecycle import producer_lifecycle
from aiokafka_foundation_kit.topics.config import TopicConfig


async def _read(consumer: AIOKafkaConsumer, count: int) -> list[bytes | None]:
    values: list[bytes | None] = []
    while len(values) < count:
        batch = await consumer.getmany(timeout_ms=10_000)
        values.extend(record.value for records in batch.values() for record in records)
    return values


# ---------------------------------------------------------------------------
# consumer_lifecycle — a raw deserializer gets past a record that is not JSON
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__raw_value_deserializer__returns_every_record_around_a_malformed_one(
    kafka_container: KafkaContainer,
    topic_name: str,
):
    # Arrange — one partition, the bad record between two good ones, all in one fetch
    bootstrap = kafka_container.get_bootstrap_server()
    sent = [b'{"order": 1}', b"not json", b'{"order": 3}']
    async with producer_lifecycle(
        BaseKafkaProducerSettings(bootstrap_servers=bootstrap),
        topics=[TopicConfig(name=topic_name, num_partitions=1, replication_factor=1)],
        auto_create_topics=True,
    ) as producer:
        for value in sent:
            await producer.send_and_wait(topic_name, value)

    settings = BaseKafkaConsumerSettings(bootstrap_servers=bootstrap, group_id=topic_name)

    # Act
    async with consumer_lifecycle(settings, topics=(topic_name,), value_deserializer=lambda raw: raw) as consumer:
        received = await _read(consumer, len(sent))

    # Assert
    assert received == sent
