"""Unit tests for aiokafka_foundation_kit.utils.lifecycle."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.errors import KafkaError

from aiokafka_foundation_kit.utils.lifecycle import managed_kafka_client

# ---------------------------------------------------------------------------
# Happy path — start/stop called, client yielded
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__happy_path__starts_and_yields_client(mock_kafka_client):
    # Arrange
    client = mock_kafka_client

    # Act
    async with managed_kafka_client(client, name="test-client") as yielded:
        pass

    # Assert
    client.start.assert_awaited_once()
    assert yielded is client


async def test__managed_kafka_client__happy_path__stops_client_in_finally(mock_kafka_client):
    # Arrange
    client = mock_kafka_client

    # Act
    async with managed_kafka_client(client, name="test-client"):
        pass

    # Assert
    client.stop.assert_awaited_once()


# ---------------------------------------------------------------------------
# start() fails or is cancelled — the half-started client is stopped
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__start_raises__client_is_stopped():
    # Arrange — aiokafka's start() bootstraps before it can fail, so the client may hold connections
    client = MagicMock()
    client.start = AsyncMock(side_effect=KafkaError("topic never appeared"))
    client.stop = AsyncMock()

    # Act
    with pytest.raises(KafkaError, match="topic never appeared"):
        async with managed_kafka_client(client, name="x"):
            pass

    # Assert
    client.stop.assert_awaited_once()


async def test__managed_kafka_client__start_raises__hooks_are_not_called():
    # Arrange
    client = MagicMock()
    client.start = AsyncMock(side_effect=KafkaError("topic never appeared"))
    client.stop = AsyncMock()
    on_started = AsyncMock()
    on_stopped = AsyncMock()

    # Act
    with pytest.raises(KafkaError):
        async with managed_kafka_client(client, name="x", on_started=on_started, on_stopped=on_stopped):
            pass

    # Assert — the client never started, so neither hook sees it
    on_started.assert_not_awaited()
    on_stopped.assert_not_awaited()


async def test__managed_kafka_client__start_cancelled__client_is_stopped():
    # Arrange — start() hangs until the surrounding task is cancelled
    client = MagicMock()
    client.start = AsyncMock(side_effect=asyncio.Event().wait)
    client.stop = AsyncMock()

    async def enter() -> None:
        async with managed_kafka_client(client, name="x"):
            pass

    task = asyncio.create_task(enter())
    await asyncio.sleep(0)

    # Act
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Assert
    client.stop.assert_awaited_once()


async def test__managed_kafka_client__start_and_stop_both_raise__start_error_propagates(caplog):
    # Arrange
    client = MagicMock()
    client.start = AsyncMock(side_effect=KafkaError("start failed"))
    client.stop = AsyncMock(side_effect=OSError("stop failed"))

    # Act
    with (
        caplog.at_level(logging.ERROR, logger="aiokafka_foundation_kit.utils.lifecycle"),
        pytest.raises(KafkaError, match="start failed"),
    ):
        async with managed_kafka_client(client, name="x"):
            pass

    # Assert — the stop error is logged, not raised over the start error
    assert [record.getMessage() for record in caplog.records] == ["Error stopping Kafka x after a failed start"]


# ---------------------------------------------------------------------------
# on_started callback
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__with_on_started__calls_on_started_after_start(mock_kafka_client):
    # Arrange
    client = mock_kafka_client
    on_started = AsyncMock()

    # Act
    async with managed_kafka_client(client, name="x", on_started=on_started):
        pass

    # Assert
    on_started.assert_awaited_once_with(client)


async def test__managed_kafka_client__without_on_started__no_error():
    # Arrange
    client = MagicMock()
    client.start = AsyncMock()
    client.stop = AsyncMock()

    # Act / Assert — no error raised
    async with managed_kafka_client(client, name="x", on_started=None):
        pass


async def test__managed_kafka_client__on_started_raises__client_is_stopped():
    # Arrange
    client = MagicMock()
    client.start = AsyncMock()
    client.stop = AsyncMock()
    on_started = AsyncMock(side_effect=RuntimeError("hook failed"))

    # Act
    with pytest.raises(RuntimeError, match="hook failed"):
        async with managed_kafka_client(client, name="x", on_started=on_started):
            pass

    # Assert — the started client must not be left running
    client.stop.assert_awaited_once()


# ---------------------------------------------------------------------------
# on_stopped callback
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__with_on_stopped__calls_on_stopped_after_stop(mock_kafka_client):
    # Arrange
    client = mock_kafka_client
    on_stopped = AsyncMock()

    # Act
    async with managed_kafka_client(client, name="x", on_stopped=on_stopped):
        pass

    # Assert
    on_stopped.assert_awaited_once_with(client)


async def test__managed_kafka_client__without_on_stopped__no_error(mock_kafka_client):
    # Arrange / Act / Assert — no error raised
    async with managed_kafka_client(mock_kafka_client, name="x", on_stopped=None):
        pass


# ---------------------------------------------------------------------------
# KafkaError during stop — caught and not re-raised
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__kafka_error_on_stop__does_not_propagate():
    # Arrange
    client = MagicMock()
    client.start = AsyncMock()
    client.stop = AsyncMock(side_effect=KafkaError("broker unavailable"))

    # Act — must not raise
    async with managed_kafka_client(client, name="x"):
        pass

    # Assert
    client.stop.assert_awaited_once()


async def test__managed_kafka_client__kafka_error_on_stop__on_stopped_not_called():
    # Arrange
    client = MagicMock()
    client.start = AsyncMock()
    client.stop = AsyncMock(side_effect=KafkaError("err"))
    on_stopped = AsyncMock()

    # Act
    async with managed_kafka_client(client, name="x", on_stopped=on_stopped):
        pass

    # Assert — on_stopped is skipped because stop() raised
    on_stopped.assert_not_awaited()


# ---------------------------------------------------------------------------
# Exception inside body — stop still called
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__exception_in_body__stop_still_called(mock_kafka_client):
    # Arrange
    client = mock_kafka_client

    # Act
    with pytest.raises(ValueError, match="boom"):
        async with managed_kafka_client(client, name="x"):
            raise ValueError("boom")

    # Assert
    client.stop.assert_awaited_once()


async def test__managed_kafka_client__exception_in_body__original_exception_propagated(mock_kafka_client):
    # Arrange / Act / Assert
    with pytest.raises(RuntimeError, match="original"):
        async with managed_kafka_client(mock_kafka_client, name="x"):
            raise RuntimeError("original")


# ---------------------------------------------------------------------------
# start/stop call order
# ---------------------------------------------------------------------------


async def test__managed_kafka_client__call_order__start_before_yield_stop_after(mock_kafka_client):
    # Arrange
    client = mock_kafka_client
    call_log: list[str] = []
    client.start = AsyncMock(side_effect=lambda: call_log.append("start"))
    client.stop = AsyncMock(side_effect=lambda: call_log.append("stop"))

    # Act
    async with managed_kafka_client(client, name="x"):
        call_log.append("body")

    # Assert
    assert call_log == ["start", "body", "stop"]
