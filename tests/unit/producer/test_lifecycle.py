"""Unit tests for aiokafka_foundation_kit.producer.lifecycle."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka_foundation_kit.producer.lifecycle import producer_lifecycle
from aiokafka_foundation_kit.topics.config import TopicConfig

# ---------------------------------------------------------------------------
# Basic lifecycle — producer yielded
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__basic__yields_producer(producer_settings):
    # Arrange
    mock_producer = MagicMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with producer_lifecycle(producer_settings) as result:
            yielded = result

    # Assert
    assert yielded is mock_producer


# ---------------------------------------------------------------------------
# auto_create_topics=False — ensure_topics_async NOT called
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__auto_create_false__does_not_call_ensure_topics(
    producer_settings,
):
    # Arrange
    mock_producer = MagicMock()
    topics = [TopicConfig(name="t", num_partitions=1, replication_factor=1)]

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.ensure_topics_async",
        ) as mock_ensure,
    ):
        # Act
        async with producer_lifecycle(producer_settings, topics=topics, auto_create_topics=False):
            pass

    # Assert
    mock_ensure.assert_not_called()


# ---------------------------------------------------------------------------
# auto_create_topics=True + topics — ensure_topics_async IS called
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__auto_create_true_with_topics__calls_ensure_topics(
    producer_settings,
):
    # Arrange
    mock_producer = MagicMock()
    topics = [TopicConfig(name="t", num_partitions=1, replication_factor=1)]

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.ensure_topics_async",
            new_callable=AsyncMock,
        ) as mock_ensure,
    ):
        # Act
        async with producer_lifecycle(producer_settings, topics=topics, auto_create_topics=True):
            pass

    # Assert
    mock_ensure.assert_awaited_once_with(topics, producer_settings)


# ---------------------------------------------------------------------------
# auto_create_topics=True + no topics — ensure_topics_async NOT called
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__auto_create_true_no_topics__does_not_call_ensure_topics(
    producer_settings,
):
    # Arrange
    mock_producer = MagicMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.ensure_topics_async",
            new_callable=AsyncMock,
        ) as mock_ensure,
    ):
        # Act
        async with producer_lifecycle(producer_settings, topics=None, auto_create_topics=True):
            pass

    # Assert
    mock_ensure.assert_not_called()


# ---------------------------------------------------------------------------
# on_started / on_stopped propagation
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__with_on_started__propagates_to_managed_client(producer_settings):
    # Arrange
    mock_producer = MagicMock()
    captured: dict = {}
    on_started = AsyncMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["on_started"] = on_started
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with producer_lifecycle(producer_settings, on_started=on_started):
            pass

    # Assert
    assert captured["on_started"] is on_started


async def test__producer_lifecycle__with_on_stopped__propagates_to_managed_client(producer_settings):
    # Arrange
    mock_producer = MagicMock()
    captured: dict = {}
    on_stopped = AsyncMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["on_stopped"] = on_stopped
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with producer_lifecycle(producer_settings, on_stopped=on_stopped):
            pass

    # Assert
    assert captured["on_stopped"] is on_stopped


# ---------------------------------------------------------------------------
# managed_kafka_client called with name="producer"
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__managed_client__uses_name_producer(producer_settings):
    # Arrange
    mock_producer = MagicMock()
    captured: dict = {}

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["name"] = name
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with producer_lifecycle(producer_settings):
            pass

    # Assert
    assert captured["name"] == "producer"


# ---------------------------------------------------------------------------
# Custom name — overrides the default
# ---------------------------------------------------------------------------


async def test__producer_lifecycle__custom_name__propagates_to_managed_client(producer_settings):
    # Arrange
    mock_producer = MagicMock()
    captured: dict = {}

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["name"] = name
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.create_async_kafka_producer",
            return_value=mock_producer,
        ),
        patch(
            "aiokafka_foundation_kit.producer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with producer_lifecycle(producer_settings, name="outbox-producer"):
            pass

    # Assert
    assert captured["name"] == "outbox-producer"
