"""JSON (de)serialization helpers with optional orjson acceleration.

Falls back to the standard library ``json`` module when ``orjson`` is not
installed. ``dumps_bytes`` passes ``bytes`` values through unchanged so callers
can pre-serialize (protobuf, avro, etc.) and still rely on the default factory.
"""

from __future__ import annotations

from typing import Any

try:
    import orjson

    HAS_ORJSON = True
except ImportError:  # pragma: no cover
    import json  # pragma: no cover

    HAS_ORJSON = False  # pragma: no cover


def dumps_bytes(value: Any) -> bytes:
    """Serialize ``value`` to ``bytes``.

    ``bytes`` are returned unchanged; everything else is JSON-encoded.
    """
    if isinstance(value, bytes):
        return value
    if HAS_ORJSON:
        return orjson.dumps(value)  # type: ignore[no-any-return]
    return json.dumps(value).encode("utf-8")


def loads_bytes(value: bytes | None) -> Any:
    """Deserialize JSON ``bytes`` to a Python object.

    Returns ``None`` for tombstone messages (``value is None``).
    """
    if value is None:
        return None
    if HAS_ORJSON:
        return orjson.loads(value)
    return json.loads(value.decode("utf-8"))
