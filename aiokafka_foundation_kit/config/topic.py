"""Kafka topic configuration protocol."""

from typing import Protocol


class TopicConfigProtocol(Protocol):
    """Protocol for topic configuration."""

    name: str
    num_partitions: int
    replication_factor: int
    replica_assignment: dict[int, list[int]] | None
    topic_configs: dict[str, str] | None
