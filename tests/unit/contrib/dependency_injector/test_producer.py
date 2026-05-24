"""Unit tests for aiokafka_foundation_kit.contrib.dependency_injector.producer."""

from __future__ import annotations

import pytest

import aiokafka_foundation_kit.contrib.dependency_injector._deps as deps_mod
from aiokafka_foundation_kit.contrib.dependency_injector.producer import (
    KafkaProducerContainer,
    _producer_resource,
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
# KafkaProducerContainer — created successfully when dep-injector installed
# ---------------------------------------------------------------------------


def test__kafka_producer_container__dep_injector_installed__creates_successfully():
    # Arrange / Act
    container = KafkaProducerContainer()

    # Assert — no exception; instance created
    assert container is not None


# ---------------------------------------------------------------------------
# _producer_resource helper — delegates to producer_lifecycle
# ---------------------------------------------------------------------------


async def test__producer_resource__uses_producer_lifecycle(producer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    mock_producer = MagicMock()

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        yield mock_producer

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        results = []
        async for item in _producer_resource(producer_settings, topics=None, auto_create_topics=False):
            results.append(item)

    # Assert
    assert results[0] is mock_producer


async def test__producer_resource__passes_auto_create_topics_flag(producer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        captured["auto_create_topics"] = auto_create_topics
        yield MagicMock()

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in _producer_resource(producer_settings, topics=None, auto_create_topics=True):
            pass

    # Assert
    assert captured["auto_create_topics"] is True


async def test__producer_resource__passes_topics(producer_settings):
    # Arrange
    from contextlib import asynccontextmanager  # noqa: PLC0415
    from unittest.mock import MagicMock, patch  # noqa: PLC0415

    mock_topics = [MagicMock()]
    captured: dict = {}

    @asynccontextmanager
    async def mock_lifecycle(settings, *, topics=None, auto_create_topics=False, **kw):
        captured["topics"] = topics
        yield MagicMock()

    with patch(
        "aiokafka_foundation_kit.contrib.dependency_injector.producer.producer_lifecycle",
        mock_lifecycle,
    ):
        # Act
        async for _ in _producer_resource(producer_settings, topics=mock_topics, auto_create_topics=False):
            pass

    # Assert
    assert captured["topics"] is mock_topics
