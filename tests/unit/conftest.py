"""Shared fixtures for unit tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


def make_kafka_settings(**overrides: object) -> MagicMock:
    """Build a MagicMock that satisfies KafkaSettingsProtocol."""
    s = MagicMock()
    s.bootstrap_servers = "localhost:9092"
    s.client_id = None
    s.security_protocol = "PLAINTEXT"
    s.metadata_max_age_ms = 300000
    s.get_sasl_password.return_value = None
    s.sasl_mechanism = None
    s.sasl_username = None
    s.ssl_cafile = None
    s.ssl_certfile = None
    s.ssl_keyfile = None
    s.ssl_check_hostname = True
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


@pytest.fixture
def plaintext_settings() -> MagicMock:
    """PLAINTEXT Kafka settings."""
    return make_kafka_settings()


@pytest.fixture
def consumer_settings() -> MagicMock:
    """Consumer settings with all required fields."""
    s = make_kafka_settings()
    s.group_id = "test-group"
    s.auto_offset_reset = "earliest"
    s.enable_auto_commit = False
    s.session_timeout_ms = 30000
    s.heartbeat_interval_ms = 3000
    s.max_poll_records = 500
    s.max_poll_interval_ms = 300000
    s.fetch_max_wait_ms = 500
    s.fetch_min_bytes = 1
    s.fetch_max_bytes = 52428800
    return s


@pytest.fixture
def producer_settings() -> MagicMock:
    """Producer settings with all required fields."""
    s = make_kafka_settings()
    s.acks = "all"
    s.compression_type = "gzip"
    s.enable_idempotence = True
    s.max_batch_size = 16384
    s.linger_ms = 5
    s.request_timeout_ms = 30000
    s.auto_create_topics = False
    return s


@pytest.fixture
def mock_kafka_client() -> MagicMock:
    """A generic mock Kafka client with async start/stop."""
    client = MagicMock()
    client.start = AsyncMock()
    client.stop = AsyncMock()
    return client
