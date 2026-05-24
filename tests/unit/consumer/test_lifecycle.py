"""Unit tests for aiokafka_foundation_kit.consumer.lifecycle."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle

# ---------------------------------------------------------------------------
# Basic lifecycle — consumer yielded
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__basic__yields_consumer(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            return_value=mock_consumer,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings) as result:
            yielded = result

    # Assert
    assert yielded is mock_consumer


# ---------------------------------------------------------------------------
# No topics — passes None to create_async_kafka_consumer
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__no_topics__passes_none_to_factory(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}

    def factory(settings, topics=None, **kw):
        captured["topics"] = topics
        return mock_consumer

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            side_effect=factory,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings, topics=None):
            pass

    # Assert
    assert captured["topics"] is None


# ---------------------------------------------------------------------------
# With topics — passed as list to factory
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__with_topics_tuple__passes_list_to_factory(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}

    def factory(settings, topics=None, **kw):
        captured["topics"] = topics
        return mock_consumer

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            side_effect=factory,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings, topics=("t1", "t2")):
            pass

    # Assert
    assert captured["topics"] == ["t1", "t2"]


# ---------------------------------------------------------------------------
# on_started / on_stopped propagated to managed_kafka_client
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__with_on_started__propagates_to_managed_client(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}
    on_started = AsyncMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["on_started"] = on_started
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            return_value=mock_consumer,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings, on_started=on_started):
            pass

    # Assert
    assert captured["on_started"] is on_started


async def test__consumer_lifecycle__with_on_stopped__propagates_to_managed_client(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}
    on_stopped = AsyncMock()

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["on_stopped"] = on_stopped
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            return_value=mock_consumer,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings, on_stopped=on_stopped):
            pass

    # Assert
    assert captured["on_stopped"] is on_stopped


# ---------------------------------------------------------------------------
# managed_kafka_client called with name="consumer"
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__managed_client__uses_name_consumer(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["name"] = name
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            return_value=mock_consumer,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings):
            pass

    # Assert
    assert captured["name"] == "consumer"


# ---------------------------------------------------------------------------
# Custom name — overrides the default
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__custom_name__propagates_to_managed_client(consumer_settings):
    # Arrange
    mock_consumer = MagicMock()
    captured: dict = {}

    @asynccontextmanager
    async def mock_managed(client, *, name, on_started=None, on_stopped=None):
        captured["name"] = name
        yield client

    with (
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.create_async_kafka_consumer",
            return_value=mock_consumer,
        ),
        patch(
            "aiokafka_foundation_kit.consumer.lifecycle.managed_kafka_client",
            mock_managed,
        ),
    ):
        # Act
        async with consumer_lifecycle(consumer_settings, name="inbox-consumer"):
            pass

    # Assert
    assert captured["name"] == "inbox-consumer"
