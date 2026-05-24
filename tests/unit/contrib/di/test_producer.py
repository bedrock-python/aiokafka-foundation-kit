"""Unit tests for aiokafka_foundation_kit.contrib.di.producer."""

from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import pytest

import aiokafka_foundation_kit.contrib.di._deps as deps_mod
from aiokafka_foundation_kit.contrib.di.producer import AsyncKafkaProducerProvider

# ---------------------------------------------------------------------------
# __init__ — check_dishka guard
# ---------------------------------------------------------------------------


def test__async_kafka_producer_provider__dishka_not_installed__raises_on_init(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dishka"):
        AsyncKafkaProducerProvider()


# ---------------------------------------------------------------------------
# get_kafka_producer — yields producer from producer_lifecycle
# ---------------------------------------------------------------------------


async def test__async_kafka_producer_provider__get_kafka_producer__yields_producer(
    producer_settings,
):
    # Arrange
    mock_producer = MagicMock()
    producer_settings.auto_create_topics = False

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        yield mock_producer

    provider = AsyncKafkaProducerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        results = []
        async for item in provider.get_kafka_producer(producer_settings, topics=None):
            results.append(item)

    # Assert
    assert results[0] is mock_producer


async def test__async_kafka_producer_provider__get_kafka_producer__passes_auto_create_from_settings(
    producer_settings,
):
    # Arrange
    producer_settings.auto_create_topics = True
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        captured["auto_create_topics"] = auto_create_topics
        yield MagicMock()

    provider = AsyncKafkaProducerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in provider.get_kafka_producer(producer_settings, topics=None):
            pass

    # Assert
    assert captured["auto_create_topics"] is True


async def test__async_kafka_producer_provider__get_kafka_producer__passes_topics(
    producer_settings,
):
    # Arrange
    producer_settings.auto_create_topics = False
    mock_topics = [MagicMock()]
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        captured["topics"] = topics
        yield MagicMock()

    provider = AsyncKafkaProducerProvider()

    with patch(
        "aiokafka_foundation_kit.contrib.di.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in provider.get_kafka_producer(producer_settings, topics=mock_topics):
            pass

    # Assert
    assert captured["topics"] is mock_topics
