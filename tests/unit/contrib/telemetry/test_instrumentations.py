"""Unit tests for aiokafka_foundation_kit.contrib.telemetry.instrumentations."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from aiokafka_foundation_kit.contrib.telemetry.instrumentations import instrument_aiokafka

# ---------------------------------------------------------------------------
# ImportError when opentelemetry not installed
# ---------------------------------------------------------------------------


def test__instrument_aiokafka__otel_not_installed__raises_import_error(monkeypatch):
    # Arrange — set the module to None to force ImportError on from-import
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.aiokafka", None)

    # Act / Assert
    with pytest.raises(ImportError):
        instrument_aiokafka()


def test__instrument_aiokafka__otel_not_installed__error_message_contains_package_name(monkeypatch):
    # Arrange
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.aiokafka", None)

    # Act / Assert
    with pytest.raises(ImportError, match="opentelemetry-instrumentation-aiokafka"):
        instrument_aiokafka()


def test__instrument_aiokafka__otel_not_installed__error_message_contains_install_hint(monkeypatch):
    # Arrange
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.aiokafka", None)

    # Act / Assert
    with pytest.raises(ImportError, match="aiokafka-foundation-kit\\[telemetry\\]"):
        instrument_aiokafka()


# ---------------------------------------------------------------------------
# Success paths — AIOKafkaInstrumentor.instrument called correctly
# ---------------------------------------------------------------------------


def _make_instrumentor_mock() -> tuple[MagicMock, MagicMock]:
    """Return (mock_module, mock_instrumentor_instance)."""
    mock_instrumentor_cls = MagicMock()
    mock_instrumentor_instance = MagicMock()
    mock_instrumentor_cls.return_value = mock_instrumentor_instance
    mock_module = MagicMock()
    mock_module.AIOKafkaInstrumentor = mock_instrumentor_cls
    return mock_module, mock_instrumentor_instance


def test__instrument_aiokafka__no_overrides__calls_instrument_with_no_extra_kwargs():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka()

    # Assert
    mock_instance.instrument.assert_called_once_with()


def test__instrument_aiokafka__with_tracer_provider__passes_tracer_provider():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()
    fake_provider = MagicMock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka(tracer_provider=fake_provider)

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert kwargs["tracer_provider"] is fake_provider


def test__instrument_aiokafka__without_tracer_provider__does_not_pass_tracer_provider_key():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka()

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert "tracer_provider" not in kwargs


def test__instrument_aiokafka__with_async_produce_hook__passes_produce_hook():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()
    hook = MagicMock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka(async_produce_hook=hook)

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert kwargs["async_produce_hook"] is hook


def test__instrument_aiokafka__without_produce_hook__does_not_pass_produce_hook_key():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka()

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert "async_produce_hook" not in kwargs


def test__instrument_aiokafka__with_async_consume_hook__passes_consume_hook():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()
    hook = MagicMock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka(async_consume_hook=hook)

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert kwargs["async_consume_hook"] is hook


def test__instrument_aiokafka__without_consume_hook__does_not_pass_consume_hook_key():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka()

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert "async_consume_hook" not in kwargs


def test__instrument_aiokafka__with_extra_kwargs__passes_extra_kwargs_to_instrument():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka(exclude_urls="http://localhost")

    # Assert
    _args, kwargs = mock_instance.instrument.call_args
    assert kwargs["exclude_urls"] == "http://localhost"


def test__instrument_aiokafka__all_overrides_combined__passes_all_to_instrument():
    # Arrange
    mock_module, mock_instance = _make_instrumentor_mock()
    provider = MagicMock()
    produce_hook = MagicMock()
    consume_hook = MagicMock()

    with patch.dict(sys.modules, {"opentelemetry.instrumentation.aiokafka": mock_module}):
        # Act
        instrument_aiokafka(
            tracer_provider=provider,
            async_produce_hook=produce_hook,
            async_consume_hook=consume_hook,
        )

    # Assert
    _, kwargs = mock_instance.instrument.call_args
    assert kwargs["tracer_provider"] is provider
    assert kwargs["async_produce_hook"] is produce_hook
    assert kwargs["async_consume_hook"] is consume_hook
