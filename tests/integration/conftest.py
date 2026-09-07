"""Shared fixtures for integration tests.

These talk to a real broker in a container, so one Kafka is started for the
whole session and every test works on topic names of its own.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from testcontainers.kafka import KafkaContainer

from aiokafka_foundation_kit.contrib.models import BaseKafkaProducerSettings
from aiokafka_foundation_kit.utils.config import build_kafka_common_config


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    """One Kafka broker for the whole integration session."""
    with KafkaContainer() as container:
        yield container


@pytest.fixture
def kafka_settings(kafka_container: KafkaContainer) -> BaseKafkaProducerSettings:
    """Producer settings pointing at the container, as the docs write them."""
    return BaseKafkaProducerSettings(bootstrap_servers=kafka_container.get_bootstrap_server())


@pytest.fixture
async def admin_client(kafka_settings: BaseKafkaProducerSettings) -> AsyncIterator[AIOKafkaAdminClient]:
    """A started admin client, used to check what the broker really did."""
    client = AIOKafkaAdminClient(**build_kafka_common_config(kafka_settings))
    await client.start()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
def topic_name() -> str:
    """A topic name no other test uses."""
    return f"afk-{uuid.uuid4().hex[:12]}"
