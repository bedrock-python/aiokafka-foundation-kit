"""Unit tests for aiokafka_foundation_kit.contrib.dependency_injector.consumer."""

from __future__ import annotations

import pytest

import aiokafka_foundation_kit.contrib.dependency_injector._deps as deps_mod
from aiokafka_foundation_kit.contrib.dependency_injector.consumer import (
    KafkaConsumerContainer,
    _consumer_resource,
)

# ---------------------------------------------------------------------------
# check_dependency_injector guard — tested via _deps module
# ---------------------------------------------------------------------------


def test__check_dependency_injector__not_installed__raises_import_error(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DEPENDENCY_INJECTOR", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dependency-injector"):
        deps_mod.check_dependency_injector()


# ---------------------------------------------------------------------------
# KafkaConsumerContainer — created successfully when dep-injector installed
# ---------------------------------------------------------------------------


def test__kafka_consumer_container__dep_injector_installed__creates_successfully():
    # Arrange / Act
    container = KafkaConsumerContainer()

    # Assert
    assert container is not None


# ---------------------------------------------------------------------------
# _consumer_resource helper — delegates to consumer_lifecycle
# ---------------------------------------------------------------------------


async def test__consumer_resource__uses_consumer_lifecycle(consumer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    mock_consumer = MagicMock()

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        yield mock_consumer

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        results = []
        async for item in _consumer_resource(consumer_settings, topics=None):
            results.append(item)

    # Assert
    assert results[0] is mock_consumer


async def test__consumer_resource__passes_topics(consumer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    topics = ("t1", "t2")
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        captured["topics"] = topics
        yield MagicMock()

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in _consumer_resource(consumer_settings, topics=topics):
            pass

    # Assert
    assert captured["topics"] == topics


async def test__consumer_resource__no_topics__passes_none(consumer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, **kw):
        captured["topics"] = topics
        yield MagicMock()

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.consumer.consumer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in _consumer_resource(consumer_settings, topics=None):
            pass

    # Assert
    assert captured["topics"] is None
