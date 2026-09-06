"""Kafka topic configuration protocol."""

from typing import Protocol


class TopicConfigProtocol(Protocol):
    """Protocol for topic configuration.

    The members are read-only so that immutable implementations — the frozen
    :class:`~aiokafka_foundation_kit.topics.config.TopicConfig` dataclass among
    them — satisfy it. A plain mutable attribute satisfies a read-only member,
    so ordinary classes and Pydantic models qualify unchanged.
    """

    @property
    def name(self) -> str: ...

    @property
    def num_partitions(self) -> int: ...

    @property
    def replication_factor(self) -> int: ...

    @property
    def replica_assignment(self) -> dict[int, list[int]] | None: ...

    @property
    def topic_configs(self) -> dict[str, str] | None: ...
