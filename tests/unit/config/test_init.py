"""Unit tests for the aiokafka_foundation_kit.config package exports."""

from __future__ import annotations

import pytest

from aiokafka_foundation_kit import config
from aiokafka_foundation_kit.config.producer import ProducerLifecycleSettingsProtocol

# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------


def test__config_package__re_exports_producer_lifecycle_settings_protocol():
    # Arrange / Act / Assert
    assert config.ProducerLifecycleSettingsProtocol is ProducerLifecycleSettingsProtocol


def test__config_all__lists_producer_lifecycle_settings_protocol():
    # Arrange / Act / Assert
    assert "ProducerLifecycleSettingsProtocol" in config.__all__


def test__config_all__every_name_is_bound_on_the_package():
    # Arrange / Act
    missing = [name for name in config.__all__ if not hasattr(config, name)]

    # Assert
    assert missing == []


# ---------------------------------------------------------------------------
# The protocols must be protocols
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "protocol",
    [
        config.KafkaConnectionSettingsProtocol,
        config.KafkaSaslSettingsProtocol,
        config.KafkaSslSettingsProtocol,
        config.KafkaSettingsProtocol,
        config.ProducerSettingsProtocol,
        config.ProducerLifecycleSettingsProtocol,
        config.ConsumerSettingsProtocol,
        config.TopicConfigProtocol,
    ],
)
def test__config_protocols__are_structural_protocols(protocol: type):
    # Arrange / Act / Assert — a subclass that omits Protocol from its bases becomes an
    # ordinary nominal class, and structural typing silently stops applying to it
    assert protocol._is_protocol is True
