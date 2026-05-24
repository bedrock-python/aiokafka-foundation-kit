"""Topic configuration."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicConfig:
    """Kafka topic configuration."""

    name: str
    num_partitions: int
    replication_factor: int
    replica_assignment: dict[int, list[int]] | None = None
    topic_configs: dict[str, str] | None = None
