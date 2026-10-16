"""Integration tests for aiokafka_foundation_kit.consumer.lifecycle against a real broker."""

from __future__ import annotations

import asyncio

import pytest
from testcontainers.kafka import KafkaContainer

from aiokafka_foundation_kit.consumer.lifecycle import consumer_lifecycle
from aiokafka_foundation_kit.contrib.models import BaseKafkaConsumerSettings


def _aiokafka_tasks() -> list[str]:
    current = asyncio.current_task()
    names = [task.get_coro().__qualname__ for task in asyncio.all_tasks() if task is not current]
    return [name for name in names if name.startswith("AIOKafka")]


# ---------------------------------------------------------------------------
# consumer_lifecycle — a start() that does not finish leaves nothing running
# ---------------------------------------------------------------------------


async def test__consumer_lifecycle__start_cancelled_waiting_for_missing_topic__leaves_no_client_running(
    kafka_container: KafkaContainer,
    topic_name: str,
):
    # Arrange — auto-creation is off, so start() bootstraps and then waits for a topic that never appears
    settings = BaseKafkaConsumerSettings(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        group_id=topic_name,
    )

    # Act
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(3), consumer_lifecycle(settings, topics=(topic_name,)):
            pass

    # Assert — no metadata synchronizer, no connection reader
    assert _aiokafka_tasks() == []
