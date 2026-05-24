"""Unit tests for aiokafka_foundation_kit.utils.json."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

import aiokafka_foundation_kit.utils.json as json_mod
from aiokafka_foundation_kit.utils.json import dumps_bytes, loads_bytes

# ---------------------------------------------------------------------------
# Helper — inject a mock json module for the HAS_ORJSON=False path
# ---------------------------------------------------------------------------


def _inject_mock_json(monkeypatch) -> MagicMock:
    """Add a mock 'json' name into the utils.json module namespace."""
    mock_json = MagicMock()
    monkeypatch.setattr(json_mod, "json", mock_json, raising=False)
    return mock_json


# ---------------------------------------------------------------------------
# dumps_bytes — bytes passthrough (independent of HAS_ORJSON)
# ---------------------------------------------------------------------------


def test__dumps_bytes__given_bytes_value__returns_same_bytes_unchanged():
    # Arrange
    raw = b"\x00\x01\x02"

    # Act
    result = dumps_bytes(raw)

    # Assert
    assert result is raw


def test__dumps_bytes__given_empty_bytes__returns_empty_bytes():
    # Arrange
    raw = b""

    # Act
    result = dumps_bytes(raw)

    # Assert
    assert result == b""


# ---------------------------------------------------------------------------
# dumps_bytes — orjson path (HAS_ORJSON=True)
# ---------------------------------------------------------------------------


def test__dumps_bytes__given_dict_with_orjson__returns_json_bytes(monkeypatch):
    # Arrange
    import orjson  # noqa: PLC0415

    monkeypatch.setattr(json_mod, "HAS_ORJSON", True)
    monkeypatch.setattr(json_mod, "orjson", orjson)
    data = {"key": "value", "num": 42}

    # Act
    result = dumps_bytes(data)

    # Assert
    assert isinstance(result, bytes)
    assert json.loads(result) == data


def test__dumps_bytes__given_list_with_orjson__returns_json_bytes(monkeypatch):
    # Arrange
    import orjson  # noqa: PLC0415

    monkeypatch.setattr(json_mod, "HAS_ORJSON", True)
    monkeypatch.setattr(json_mod, "orjson", orjson)

    # Act
    result = dumps_bytes([1, 2, 3])

    # Assert
    assert result == b"[1,2,3]"


# ---------------------------------------------------------------------------
# dumps_bytes — stdlib json path (HAS_ORJSON=False)
# ---------------------------------------------------------------------------


def test__dumps_bytes__given_dict_without_orjson__returns_json_bytes(monkeypatch):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.dumps.return_value = '{"key": "value"}'
    data = {"key": "value"}

    # Act
    result = dumps_bytes(data)

    # Assert
    assert result == b'{"key": "value"}'
    mock_json.dumps.assert_called_once_with(data)


def test__dumps_bytes__given_string_without_orjson__returns_json_encoded_bytes(monkeypatch):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.dumps.return_value = '"hello"'

    # Act
    result = dumps_bytes("hello")

    # Assert
    assert result == b'"hello"'


def test__dumps_bytes__given_int_without_orjson__returns_json_bytes(monkeypatch):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.dumps.return_value = "99"

    # Act
    result = dumps_bytes(99)

    # Assert
    assert result == b"99"


# ---------------------------------------------------------------------------
# loads_bytes — None tombstone (independent of HAS_ORJSON)
# ---------------------------------------------------------------------------


def test__loads_bytes__given_none__returns_none():
    # Arrange / Act
    result = loads_bytes(None)

    # Assert
    assert result is None


# ---------------------------------------------------------------------------
# loads_bytes — orjson path
# ---------------------------------------------------------------------------


def test__loads_bytes__given_json_bytes_with_orjson__returns_dict(monkeypatch):
    # Arrange
    import orjson  # noqa: PLC0415

    monkeypatch.setattr(json_mod, "HAS_ORJSON", True)
    monkeypatch.setattr(json_mod, "orjson", orjson)

    # Act
    result = loads_bytes(b'{"a":1}')

    # Assert
    assert result == {"a": 1}


def test__loads_bytes__given_json_list_bytes_with_orjson__returns_list(monkeypatch):
    # Arrange
    import orjson  # noqa: PLC0415

    monkeypatch.setattr(json_mod, "HAS_ORJSON", True)
    monkeypatch.setattr(json_mod, "orjson", orjson)

    # Act
    result = loads_bytes(b"[1,2,3]")

    # Assert
    assert result == [1, 2, 3]


# ---------------------------------------------------------------------------
# loads_bytes — stdlib json path
# ---------------------------------------------------------------------------


def test__loads_bytes__given_json_bytes_without_orjson__returns_dict(monkeypatch):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.loads.return_value = {"x": 42}

    # Act
    result = loads_bytes(b'{"x":42}')

    # Assert
    assert result == {"x": 42}
    mock_json.loads.assert_called_once_with('{"x":42}')


def test__loads_bytes__given_string_json_bytes_without_orjson__returns_string(monkeypatch):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.loads.return_value = "hello"

    # Act
    result = loads_bytes(b'"hello"')

    # Assert
    assert result == "hello"


# ---------------------------------------------------------------------------
# Parametrize — round-trip with orjson
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        {"key": "val"},
        [1, 2, 3],
        42,
        "text",
        True,
    ],
)
def test__dumps_bytes_loads_bytes__round_trip_with_orjson(monkeypatch, value):
    # Arrange
    import orjson  # noqa: PLC0415

    monkeypatch.setattr(json_mod, "HAS_ORJSON", True)
    monkeypatch.setattr(json_mod, "orjson", orjson)

    # Act
    serialized = dumps_bytes(value)
    deserialized = loads_bytes(serialized)

    # Assert
    assert deserialized == value


# ---------------------------------------------------------------------------
# Parametrize — round-trip using stdlib json via mock
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,json_str",
    [
        ({"key": "val"}, '{"key": "val"}'),
        ([1, 2, 3], "[1, 2, 3]"),
        (42, "42"),
        ("text", '"text"'),
    ],
)
def test__dumps_bytes_loads_bytes__round_trip_without_orjson(monkeypatch, value, json_str):
    # Arrange
    monkeypatch.setattr(json_mod, "HAS_ORJSON", False)
    mock_json = _inject_mock_json(monkeypatch)
    mock_json.dumps.return_value = json_str
    mock_json.loads.return_value = value

    # Act
    serialized = dumps_bytes(value)
    deserialized = loads_bytes(serialized)

    # Assert
    assert deserialized == value
