"""Unit tests for aiokafka_foundation_kit.topics.management."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import TopicAlreadyExistsError

from aiokafka_foundation_kit.topics.config import TopicConfig
from aiokafka_foundation_kit.topics.management import _to_new_topic, ensure_topics_async

# ---------------------------------------------------------------------------
# _to_new_topic — tested by patching NewTopic (avoids aiokafka validation)
# ---------------------------------------------------------------------------


def test__to_new_topic__minimal_topic_config__maps_name():
    # Arrange
    topic = TopicConfig(name="my-topic", num_partitions=3, replication_factor=2)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["name"] == "my-topic"
    assert kwargs["num_partitions"] == 3
    assert kwargs["replication_factor"] == 2


def test__to_new_topic__without_replica_assignment__uses_empty_dict():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1, replica_assignment=None)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["replica_assignments"] == {}


def test__to_new_topic__with_replica_assignment__passes_through():
    # Arrange
    assignment = {0: [1, 2]}
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=2, replica_assignment=assignment)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["replica_assignments"] == assignment


def test__to_new_topic__without_topic_configs__uses_empty_dict():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1, topic_configs=None)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["topic_configs"] == {}


def test__to_new_topic__with_topic_configs__passes_through():
    # Arrange
    configs = {"retention.ms": "86400000"}
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1, topic_configs=configs)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["topic_configs"] == configs


def test__to_new_topic__returns_new_topic_instance():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1)
    mock_new_topic = MagicMock()

    # Act
    with patch(
        "aiokafka_foundation_kit.topics.management.NewTopic",
        return_value=mock_new_topic,
    ):
        result = _to_new_topic(topic)

    # Assert
    assert result is mock_new_topic


# ---------------------------------------------------------------------------
# ensure_topics_async — empty list → early return
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__empty_topics_list__returns_without_creating_admin_client(
    plaintext_settings,
):
    # Arrange
    with patch("aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient") as mock_admin_cls:
        # Act
        await ensure_topics_async([], plaintext_settings)

    # Assert — admin client never created
    mock_admin_cls.assert_not_called()


# ---------------------------------------------------------------------------
# ensure_topics_async — creates topics
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__single_topic__calls_create_topics_once(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock()

    mock_new_topic = MagicMock()
    topics = [TopicConfig(name="events", num_partitions=3, replication_factor=1)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        patch(
            "aiokafka_foundation_kit.topics.management.NewTopic",
            return_value=mock_new_topic,
        ),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert
    mock_admin.start.assert_awaited_once()
    mock_admin.create_topics.assert_awaited_once()


async def test__ensure_topics_async__multiple_topics__calls_create_topics_per_topic(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock()

    mock_new_topic = MagicMock()
    topics = [
        TopicConfig(name="t1", num_partitions=1, replication_factor=1),
        TopicConfig(name="t2", num_partitions=2, replication_factor=1),
        TopicConfig(name="t3", num_partitions=3, replication_factor=1),
    ]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        patch(
            "aiokafka_foundation_kit.topics.management.NewTopic",
            return_value=mock_new_topic,
        ),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert — one call per topic
    assert mock_admin.create_topics.await_count == 3


# ---------------------------------------------------------------------------
# ensure_topics_async — TopicAlreadyExistsError is silently swallowed
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__topic_already_exists__does_not_raise(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock(side_effect=TopicAlreadyExistsError())

    mock_new_topic = MagicMock()
    topics = [TopicConfig(name="existing", num_partitions=1, replication_factor=1)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        patch(
            "aiokafka_foundation_kit.topics.management.NewTopic",
            return_value=mock_new_topic,
        ),
    ):
        # Act / Assert — no exception raised
        await ensure_topics_async(topics, plaintext_settings)


async def test__ensure_topics_async__mixed_new_and_existing__continues_after_exists(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    call_count = 0

    async def create_side_effect(new_topics, validate_only):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TopicAlreadyExistsError()

    mock_admin.create_topics = AsyncMock(side_effect=create_side_effect)

    mock_new_topic = MagicMock()
    topics = [
        TopicConfig(name="existing", num_partitions=1, replication_factor=1),
        TopicConfig(name="new-topic", num_partitions=1, replication_factor=1),
    ]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        patch(
            "aiokafka_foundation_kit.topics.management.NewTopic",
            return_value=mock_new_topic,
        ),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert — both topics processed
    assert call_count == 2


# ---------------------------------------------------------------------------
# ensure_topics_async — close always called (finally block)
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__create_raises_unexpected_error__close_still_called(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock(side_effect=RuntimeError("unexpected"))

    mock_new_topic = MagicMock()
    topics = [TopicConfig(name="t", num_partitions=1, replication_factor=1)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        patch(
            "aiokafka_foundation_kit.topics.management.NewTopic",
            return_value=mock_new_topic,
        ),
        pytest.raises(RuntimeError, match="unexpected"),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert — close still called in finally
    mock_admin.close.assert_awaited_once()
