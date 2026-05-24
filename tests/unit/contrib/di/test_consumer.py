"""Unit tests for aiokafka_foundation_kit.contrib.di.consumer."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

import aiokafka_foundation_kit.contrib.di._deps as deps_mod
from aiokafka_foundation_kit.contrib.di.consumer import AsyncKafkaConsumerProvider

# ---------------------------------------------------------------------------
# __init__ — check_dishka guard
# ---------------------------------------------------------------------------


def test__async_kafka_consumer_provider__dishka_not_installed__raises_on_init(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dishka"):
        AsyncKafkaConsumerProvider()


# ---------------------------------------------------------------------------
# get_kafka_consumer — yields consumer from consumer_lifecycle
# ---------------------------------------------------------------------------


async def test__async_kafka_consumer_provider__get_kafka_consumer__yields_consumer(
    consumer_settings,
):
    # Arrange
    mock_consumer = MagicMock()

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        yield mock_consumer

    provider = AsyncKafkaConsumerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        results = []
        async for item in provider.get_kafka_consumer(consumer_settings, topics=None):
            results.append(item)

    # Assert
    assert results[0] is mock_consumer


async def test__async_kafka_consumer_provider__get_kafka_consumer__passes_topics(
    consumer_settings,
):
    # Arrange
    topics = ("t1", "t2")
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        captured["topics"] = topics
        yield MagicMock()

    provider = AsyncKafkaConsumerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in provider.get_kafka_consumer(consumer_settings, topics=topics):
            pass

    # Assert
    assert captured["topics"] == topics


async def test__async_kafka_consumer_provider__get_kafka_consumer__no_topics__passes_none(
    consumer_settings,
):
    # Arrange
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        captured["topics"] = topics
        yield MagicMock()

    provider = AsyncKafkaConsumerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in provider.get_kafka_consumer(consumer_settings, topics=None):
            pass

    # Assert
    assert captured["topics"] is None
