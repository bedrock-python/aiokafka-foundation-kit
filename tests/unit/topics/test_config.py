"""Unit tests for aiokafka_foundation_kit.topics.config."""

from __future__ import annotations

import dataclasses

import pytest

from aiokafka_foundation_kit.topics.config import TopicConfig

# ---------------------------------------------------------------------------
# Creation — required fields
# ---------------------------------------------------------------------------


def test__topic_config__created_with_required_fields__stores_correct_values():
    # Arrange / Act
    topic = TopicConfig(name="my-topic", num_partitions=3, replication_factor=2)

    # Assert
    assert topic.name == "my-topic"
    assert topic.num_partitions == 3
    assert topic.replication_factor == 2


def test__topic_config__created_without_optional_fields__defaults_to_none():
    # Arrange / Act
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1)

    # Assert
    assert topic.replica_assignment is None
    assert topic.topic_configs is None


# ---------------------------------------------------------------------------
# Optional fields
# ---------------------------------------------------------------------------


def test__topic_config__created_with_replica_assignment__stores_value():
    # Arrange
    assignment = {0: [1, 2], 1: [2, 3]}

    # Act
    topic = TopicConfig(
        name="t",
        num_partitions=2,
        replication_factor=2,
        replica_assignment=assignment,
    )

    # Assert
    assert topic.replica_assignment == assignment


def test__topic_config__created_with_topic_configs__stores_value():
    # Arrange
    configs = {"retention.ms": "86400000", "cleanup.policy": "delete"}

    # Act
    topic = TopicConfig(
        name="t",
        num_partitions=1,
        replication_factor=1,
        topic_configs=configs,
    )

    # Assert
    assert topic.topic_configs == configs


# ---------------------------------------------------------------------------
# Frozen — mutation is forbidden
# ---------------------------------------------------------------------------


def test__topic_config__is_frozen__raises_on_attribute_mutation():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1)

    # Act / Assert
    with pytest.raises(dataclasses.FrozenInstanceError):
        topic.name = "other"  # type: ignore[misc]


def test__topic_config__is_frozen__raises_on_partition_mutation():
    # Arrange
    topic = TopicConfig(name="t", num_partitions=1, replication_factor=1)

    # Act / Assert
    with pytest.raises(dataclasses.FrozenInstanceError):
        topic.num_partitions = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Equality — dataclass default
# ---------------------------------------------------------------------------


def test__topic_config__equal_instances__are_equal():
    # Arrange
    t1 = TopicConfig(name="t", num_partitions=3, replication_factor=2)
    t2 = TopicConfig(name="t", num_partitions=3, replication_factor=2)

    # Assert
    assert t1 == t2


def test__topic_config__different_names__are_not_equal():
    # Arrange
    t1 = TopicConfig(name="a", num_partitions=1, replication_factor=1)
    t2 = TopicConfig(name="b", num_partitions=1, replication_factor=1)

    # Assert
    assert t1 != t2


# ---------------------------------------------------------------------------
# Parametrize — various partition / replication values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "num_partitions,replication_factor",
    [
        (1, 1),
        (3, 2),
        (12, 3),
        (100, 1),
    ],
)
def test__topic_config__parametrized_partition_replication__stores_correct_values(
    num_partitions: int, replication_factor: int
):
    # Arrange / Act
    topic = TopicConfig(
        name="bench",
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )

    # Assert
    assert topic.num_partitions == num_partitions
    assert topic.replication_factor == replication_factor
