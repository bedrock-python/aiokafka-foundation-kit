"""Topic configuration."""

from dataclasses import dataclass


@dataclass(frozen=True)
class TopicConfig:
    """Kafka topic configuration.

    Attributes:
        name: Physical topic name, prefix already applied.
        num_partitions: Partition count, or ``-1`` when ``replica_assignment``
            is given and should decide it.
        replication_factor: Replicas per partition, or ``-1`` when
            ``replica_assignment`` is given and should decide it.
        replica_assignment: Optional partition id to broker ids mapping. When
            given it decides the shape of the topic, and the two counts above
            may only repeat what it says or be ``-1``.
        topic_configs: Optional broker-side topic overrides, e.g.
            ``{"retention.ms": "604800000"}``.
    """

    name: str
    num_partitions: int
    replication_factor: int
    replica_assignment: dict[int, list[int]] | None = None
    topic_configs: dict[str, str] | None = None
