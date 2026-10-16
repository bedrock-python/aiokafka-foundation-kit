"""Unit tests for aiokafka_foundation_kit.health.checks."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from aiokafka_foundation_kit.health.checks import check_kafka_health_async

# ---------------------------------------------------------------------------
# Healthy path
# ---------------------------------------------------------------------------


async def test__check_kafka_health_async__producer_starts_and_stops__returns_true(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        result = await check_kafka_health_async(plaintext_settings)

    # Assert
    assert result is True
    mock_producer.start.assert_awaited_once()
    mock_producer.stop.assert_awaited_once()


async def test__check_kafka_health_async__custom_timeout__sets_request_timeout_ms(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    captured_kwargs: dict = {}

    def capture(**kwargs):
        captured_kwargs.update(kwargs)
        return mock_producer

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        side_effect=capture,
    ):
        # Act
        await check_kafka_health_async(plaintext_settings, timeout_seconds=2.5)

    # Assert
    assert captured_kwargs["request_timeout_ms"] == 2500


async def test__check_kafka_health_async__default_timeout__sets_5000ms(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    captured_kwargs: dict = {}

    def capture(**kwargs):
        captured_kwargs.update(kwargs)
        return mock_producer

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        side_effect=capture,
    ):
        # Act
        await check_kafka_health_async(plaintext_settings)

    # Assert
    assert captured_kwargs["request_timeout_ms"] == 5000


# ---------------------------------------------------------------------------
# Error paths — returns False
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timeout"),
        KafkaError("connection refused"),
        OSError("network unreachable"),
    ],
)
async def test__check_kafka_health_async__exception_during_start__returns_false(plaintext_settings, exc: Exception):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock(side_effect=exc)

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        result = await check_kafka_health_async(plaintext_settings)

    # Assert
    assert result is False


async def test__check_kafka_health_async__timeout_error_on_stop__returns_false(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock(side_effect=TimeoutError("stop timeout"))

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        result = await check_kafka_health_async(plaintext_settings)

    # Assert
    assert result is False


async def test__check_kafka_health_async__kafka_error_on_stop__returns_false(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock(side_effect=KafkaError("stop failed"))

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        result = await check_kafka_health_async(plaintext_settings)

    # Assert
    assert result is False


async def test__check_kafka_health_async__os_error_on_stop__returns_false(
    plaintext_settings,
):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock(side_effect=OSError("broken pipe"))

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        result = await check_kafka_health_async(plaintext_settings)

    # Assert
    assert result is False


# ---------------------------------------------------------------------------
# A failed or cancelled start still stops the producer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timeout"),
        KafkaError("connection refused"),
        OSError("network unreachable"),
    ],
)
async def test__check_kafka_health_async__exception_during_start__stops_producer(plaintext_settings, exc: Exception):
    # Arrange
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock(side_effect=exc)
    mock_producer.stop = AsyncMock()

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        # Act
        await check_kafka_health_async(plaintext_settings)

    # Assert
    mock_producer.stop.assert_awaited_once()


async def test__check_kafka_health_async__start_cancelled__stops_producer_and_propagates(plaintext_settings):
    # Arrange — start() hangs until the probe's task is cancelled
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock(side_effect=asyncio.Event().wait)
    mock_producer.stop = AsyncMock()

    with patch(
        "aiokafka_foundation_kit.health.checks.AIOKafkaProducer",
        return_value=mock_producer,
    ):
        task = asyncio.create_task(check_kafka_health_async(plaintext_settings))
        await asyncio.sleep(0)

        # Act
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # Assert
    mock_producer.stop.assert_awaited_once()


async def test__check_kafka_health_async__real_producer_nothing_listening__returns_false_and_stops(plaintext_settings):
    # Arrange — a real aiokafka producer against a port nothing listens on
    plaintext_settings.bootstrap_servers = "127.0.0.1:1"

    with patch.object(AIOKafkaProducer, "stop", autospec=True, side_effect=AIOKafkaProducer.stop) as stop:
        # Act
        result = await check_kafka_health_async(plaintext_settings, timeout_seconds=1.0)

    # Assert — aiokafka's stop() is safe on a producer whose start() failed
    assert result is False
    stop.assert_awaited_once()
