"""Unit tests for aiokafka_foundation_kit.topics.management."""

from __future__ import annotations

import logging
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiokafka.errors import InvalidReplicationFactorError, InvalidTopicError, KafkaTimeoutError

from aiokafka_foundation_kit.topics.config import TopicConfig
from aiokafka_foundation_kit.topics.management import _to_new_topic, ensure_topics_async


def create_response(*topic_errors: tuple) -> SimpleNamespace:
    """The reply shape ``create_topics`` returns: one error entry per topic."""
    return SimpleNamespace(topic_errors=list(topic_errors))


def described_topic(name: str, replicas: list[list[int]], error_code: int = 0) -> dict:
    """The shape ``describe_topics`` returns for one topic: one entry per partition."""
    partitions = [
        {"error_code": 0, "partition": index, "leader": brokers[0], "replicas": brokers, "isr": brokers}
        for index, brokers in enumerate(replicas)
    ]
    return {"error_code": error_code, "topic": name, "is_internal": False, "partitions": partitions}


def configs_response(name: str, entries: dict[str, str], error_code: int = 0, message: str = "") -> SimpleNamespace:
    """The shape ``describe_configs`` returns for one topic resource (type 2)."""
    rows = [(key, value, False, 1, False, []) for key, value in entries.items()]
    return SimpleNamespace(resources=[(error_code, message, 2, name, rows)])


def existing_topic_admin(described: dict, configs: SimpleNamespace | None = None) -> MagicMock:
    """An admin client whose create_topics answers 36 (TOPIC_ALREADY_EXISTS)."""
    admin = MagicMock()
    admin.start = AsyncMock()
    admin.close = AsyncMock()
    admin.create_topics = AsyncMock(
        return_value=create_response((described["topic"], 36, f"Topic '{described['topic']}' already exists."))
    )
    admin.describe_topics = AsyncMock(return_value=[described])
    admin.describe_configs = AsyncMock(return_value=[configs])
    return admin


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


def test__to_new_topic__without_replica_assignment__passes_none():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1, replica_assignment=None)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert — aiokafka rejects a NewTopic that carries both counts and an assignment
    assert kwargs["replica_assignments"] is None


def test__to_new_topic__empty_replica_assignment__passes_none():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1, replica_assignment={})

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["replica_assignments"] is None
    assert kwargs["num_partitions"] == 1


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


def test__to_new_topic__with_replica_assignment__lets_the_assignment_decide_the_counts():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=2, replication_factor=1, replica_assignment={0: [1], 1: [2]})

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert — the assignment is the authority, so both counts go as the -1 aiokafka expects
    assert kwargs["num_partitions"] == -1
    assert kwargs["replication_factor"] == -1


def test__to_new_topic__with_replica_assignment_and_sentinel_counts__passes_through():
    # Arrange — a caller who spells the aiokafka rule out themselves
    assignment = {0: [1], 1: [2]}
    topic = TopicConfig(name="t", num_partitions=-1, replication_factor=-1, replica_assignment=assignment)

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["replica_assignments"] == assignment
    assert kwargs["num_partitions"] == -1


def test__to_new_topic__num_partitions_contradicts_assignment__raises_value_error():
    # Arrange — six partitions asked for, three assigned
    topic = TopicConfig(
        name="orders",
        num_partitions=6,
        replication_factor=1,
        replica_assignment={0: [1], 1: [1], 2: [1]},
    )

    # Act / Assert
    with pytest.raises(ValueError, match="describes 3 partition"):
        _to_new_topic(topic)


def test__to_new_topic__replication_factor_contradicts_assignment__raises_value_error():
    # Arrange
    topic = TopicConfig(name="orders", num_partitions=2, replication_factor=3, replica_assignment={0: [1], 1: [2]})

    # Act / Assert
    with pytest.raises(ValueError, match="1 replica"):
        _to_new_topic(topic)


def test__to_new_topic__uneven_assignment__leaves_the_replication_factor_to_the_broker():
    # Arrange — no single replication factor describes this assignment, so only the broker can judge it
    topic = TopicConfig(name="t", num_partitions=2, replication_factor=2, replica_assignment={0: [1, 2], 1: [1]})

    # Act
    with patch("aiokafka_foundation_kit.topics.management.NewTopic") as mock_cls:
        _to_new_topic(topic)
        _, kwargs = mock_cls.call_args

    # Assert
    assert kwargs["replication_factor"] == -1


# ---------------------------------------------------------------------------
# _to_new_topic — against the real NewTopic, which validates its arguments
# ---------------------------------------------------------------------------


def test__to_new_topic__plain_config_and_real_new_topic__is_accepted():
    # Arrange
    topic = TopicConfig(name="orders", num_partitions=6, replication_factor=1)

    # Act
    new_topic = _to_new_topic(topic)

    # Assert
    assert new_topic.name == "orders"
    assert new_topic.num_partitions == 6
    assert new_topic.replication_factor == 1
    assert new_topic.replica_assignments == {}


def test__to_new_topic__assignment_and_real_new_topic__is_accepted():
    # Arrange
    assignment = {0: [1], 1: [1], 2: [1]}
    topic = TopicConfig(name="orders", num_partitions=3, replication_factor=1, replica_assignment=assignment)

    # Act
    new_topic = _to_new_topic(topic)

    # Assert
    assert new_topic.num_partitions == -1
    assert new_topic.replication_factor == -1
    assert new_topic.replica_assignments == assignment


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
    # Arrange — the broker answers 36 (TOPIC_ALREADY_EXISTS) inside a successful response
    mock_admin = existing_topic_admin(described_topic("existing", [[1]]))

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
            return create_response(("existing", 36, "Topic 'existing' already exists."))
        return create_response(("new-topic", 0, None))

    mock_admin.create_topics = AsyncMock(side_effect=create_side_effect)
    mock_admin.describe_topics = AsyncMock(return_value=[described_topic("existing", [[1]])])

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
# ensure_topics_async — an existing topic that differs from its TopicConfig is reported
# ---------------------------------------------------------------------------


async def _ensure_existing(admin: MagicMock, topic: TopicConfig, settings) -> None:
    with patch("aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient", return_value=admin):
        await ensure_topics_async([topic], settings)


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]


async def test__ensure_topics_async__existing_topic_matches__logs_no_warning(plaintext_settings, caplog):
    # Arrange
    admin = existing_topic_admin(described_topic("orders", [[1], [1], [1]]))
    topic = TopicConfig(name="orders", num_partitions=3, replication_factor=1)

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert — and with no topic_configs there is nothing to describe_configs for
    assert _warnings(caplog) == []
    admin.describe_configs.assert_not_awaited()


async def test__ensure_topics_async__existing_topic_has_fewer_partitions__warns(plaintext_settings, caplog):
    # Arrange — the reporter's second deploy: 6 partitions asked for, 3 on the broker
    admin = existing_topic_admin(described_topic("orders", [[1], [1], [1]]))
    topic = TopicConfig(name="orders", num_partitions=6, replication_factor=1)

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with num_partitions=3 (TopicConfig asks for 6); it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_has_other_replication_factor__warns(plaintext_settings, caplog):
    # Arrange
    admin = existing_topic_admin(described_topic("orders", [[1, 2], [2, 3]]))
    topic = TopicConfig(name="orders", num_partitions=2, replication_factor=3)

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with replication_factor=2 (TopicConfig asks for 3); it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_has_uneven_replication__warns_with_the_range(
    plaintext_settings, caplog
):
    # Arrange — a reassignment left the partitions with different replica counts
    admin = existing_topic_admin(described_topic("orders", [[1, 2], [1, 2, 3]]))
    topic = TopicConfig(name="orders", num_partitions=2, replication_factor=3)

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with replication_factor=2-3 (TopicConfig asks for 3); it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_has_other_topic_config__warns(plaintext_settings, caplog):
    # Arrange
    admin = existing_topic_admin(
        described_topic("orders", [[1]]),
        configs_response("orders", {"retention.ms": "604800000", "cleanup.policy": "delete"}),
    )
    topic = TopicConfig(
        name="orders",
        num_partitions=1,
        replication_factor=1,
        topic_configs={"retention.ms": "3600000", "cleanup.policy": "delete"},
    )

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with retention.ms=604800000 (TopicConfig asks for 3600000); "
        "it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_with_topic_configs__describes_only_those_keys(
    plaintext_settings,
):
    # Arrange
    admin = existing_topic_admin(
        described_topic("orders", [[1]]),
        configs_response("orders", {"retention.ms": "3600000"}),
    )
    topic = TopicConfig(
        name="orders", num_partitions=1, replication_factor=1, topic_configs={"retention.ms": "3600000"}
    )

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    ((resources,), _) = admin.describe_configs.await_args
    assert [(resource.name, resource.configs) for resource in resources] == [("orders", {"retention.ms": None})]


async def test__ensure_topics_async__existing_topic_lacks_a_topic_config_key__warns_it_is_unset(
    plaintext_settings, caplog
):
    # Arrange — the broker reports no entry for a key it does not know
    admin = existing_topic_admin(described_topic("orders", [[1]]), configs_response("orders", {}))
    topic = TopicConfig(name="orders", num_partitions=1, replication_factor=1, topic_configs={"made.up": "1"})

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with made.up unset (TopicConfig asks for 1); it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_differs_everywhere__warns_once_with_every_difference(
    plaintext_settings, caplog
):
    # Arrange
    admin = existing_topic_admin(
        described_topic("orders", [[1], [1], [1]]),
        configs_response("orders", {"retention.ms": "604800000"}),
    )
    topic = TopicConfig(
        name="orders", num_partitions=6, replication_factor=2, topic_configs={"retention.ms": "3600000"}
    )

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with num_partitions=3 (TopicConfig asks for 6), "
        "replication_factor=1 (TopicConfig asks for 2), retention.ms=604800000 (TopicConfig asks for 3600000); "
        "it was left unchanged"
    ]


async def test__ensure_topics_async__existing_topic_and_replica_assignment__compares_its_counts(
    plaintext_settings, caplog
):
    # Arrange — the assignment asks for 3 partitions of 2 replicas; placement is not compared
    admin = existing_topic_admin(described_topic("orders", [[2, 3], [3, 1]]))
    topic = TopicConfig(
        name="orders",
        num_partitions=-1,
        replication_factor=-1,
        replica_assignment={0: [1, 2], 1: [2, 3], 2: [3, 1]},
    )

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Topic 'orders' already exists with num_partitions=2 (TopicConfig asks for 3); it was left unchanged"
    ]


async def test__ensure_topics_async__describe_configs_refused__warns_and_does_not_raise(plaintext_settings, caplog):
    # Arrange — 29 is TOPIC_AUTHORIZATION_FAILED: allowed to create, not to describe configs
    admin = existing_topic_admin(
        described_topic("orders", [[1]]),
        configs_response("orders", {}, error_code=29, message="Not authorized"),
    )
    topic = TopicConfig(name="orders", num_partitions=1, replication_factor=1, topic_configs={"retention.ms": "1"})

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Could not compare existing topic 'orders' with its TopicConfig: "
        "[Error 29] TopicAuthorizationFailedError: Not authorized"
    ]


async def test__ensure_topics_async__describe_topics_raises__warns_and_does_not_raise(plaintext_settings, caplog):
    # Arrange
    admin = existing_topic_admin(described_topic("orders", [[1]]))
    admin.describe_topics = AsyncMock(side_effect=KafkaTimeoutError("metadata request timed out"))
    topic = TopicConfig(name="orders", num_partitions=1, replication_factor=1)

    # Act
    await _ensure_existing(admin, topic, plaintext_settings)

    # Assert
    assert _warnings(caplog) == [
        "Could not compare existing topic 'orders' with its TopicConfig: KafkaTimeoutError: metadata request timed out"
    ]


# ---------------------------------------------------------------------------
# ensure_topics_async — a refused topic is an error code, not a raised error
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__broker_refuses_topic__raises_the_error_it_named(
    plaintext_settings,
):
    # Arrange — 38 is INVALID_REPLICATION_FACTOR
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock(
        return_value=create_response(("events", 38, "Replication factor: 3 larger than available brokers: 1."))
    )

    topics = [TopicConfig(name="events", num_partitions=1, replication_factor=3)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        pytest.raises(InvalidReplicationFactorError, match="larger than available brokers"),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)


async def test__ensure_topics_async__refused_topic__does_not_attempt_the_rest(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock(return_value=create_response(("in valid!", 17, "Topic name is invalid")))

    topics = [
        TopicConfig(name="in valid!", num_partitions=1, replication_factor=1),
        TopicConfig(name="never-attempted", num_partitions=1, replication_factor=1),
    ]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        pytest.raises(InvalidTopicError),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert
    assert mock_admin.create_topics.await_count == 1
    mock_admin.close.assert_awaited_once()


async def test__ensure_topics_async__response_without_error_message__raises_with_a_message_of_its_own(
    plaintext_settings,
):
    # Arrange — the v0 response carries no message, only a code
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock(return_value=create_response(("events", 38)))

    topics = [TopicConfig(name="events", num_partitions=1, replication_factor=3)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        pytest.raises(InvalidReplicationFactorError, match="Broker refused topic 'events'"),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)


# ---------------------------------------------------------------------------
# ensure_topics_async — a contradicting config never reaches the broker
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__counts_contradict_assignment__raises_before_creating(
    plaintext_settings,
):
    # Arrange
    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock()

    topics = [
        TopicConfig(name="orders", num_partitions=6, replication_factor=1, replica_assignment={0: [1], 1: [1]}),
    ]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ),
        pytest.raises(ValueError, match="num_partitions is 6"),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert
    mock_admin.create_topics.assert_not_awaited()
    mock_admin.close.assert_awaited_once()


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


# ---------------------------------------------------------------------------
# TLS — the admin client only accepts a prepared ssl_context
# ---------------------------------------------------------------------------


async def test__ensure_topics_async__ssl_settings__passes_ssl_context_to_admin_client(
    plaintext_settings,
):
    # Arrange — no cafile, so the system trust store is used and no file is read
    plaintext_settings.security_protocol = "SSL"
    plaintext_settings.ssl_cafile = None

    mock_admin = MagicMock()
    mock_admin.start = AsyncMock()
    mock_admin.close = AsyncMock()
    mock_admin.create_topics = AsyncMock()

    topics = [TopicConfig(name="events", num_partitions=1, replication_factor=1)]

    with (
        patch(
            "aiokafka_foundation_kit.topics.management.AIOKafkaAdminClient",
            return_value=mock_admin,
        ) as mock_admin_cls,
        patch("aiokafka_foundation_kit.topics.management.NewTopic", return_value=MagicMock()),
    ):
        # Act
        await ensure_topics_async(topics, plaintext_settings)

    # Assert
    _, kwargs = mock_admin_cls.call_args
    assert isinstance(kwargs["ssl_context"], ssl.SSLContext)
    assert "ssl_cafile" not in kwargs
    assert "ssl_check_hostname" not in kwargs
