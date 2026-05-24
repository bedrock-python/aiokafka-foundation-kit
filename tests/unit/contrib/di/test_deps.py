"""Unit tests for aiokafka_foundation_kit.contrib.di._deps."""

from __future__ import annotations

import pytest

import aiokafka_foundation_kit.contrib.di._deps as deps_mod
from aiokafka_foundation_kit.contrib.di._deps import check_dishka

# ---------------------------------------------------------------------------
# check_dishka — installed
# ---------------------------------------------------------------------------


def test__check_dishka__dishka_installed__does_not_raise(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", True)

    # Act / Assert — no exception
    check_dishka()


# ---------------------------------------------------------------------------
# check_dishka — not installed
# ---------------------------------------------------------------------------


def test__check_dishka__dishka_not_installed__raises_import_error(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError):
        check_dishka()


def test__check_dishka__dishka_not_installed__message_contains_dishka(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dishka"):
        check_dishka()


def test__check_dishka__dishka_not_installed__message_contains_install_hint(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DISHKA", False)

    # Act / Assert
    with pytest.raises(ImportError, match="aiokafka-foundation-kit\\[dishka\\]"):
        check_dishka()


# ---------------------------------------------------------------------------
# HAS_DISHKA flag reflects actual import state
# ---------------------------------------------------------------------------


def test__deps_module__has_dishka_flag__is_bool():
    # Arrange / Act / Assert
    assert isinstance(deps_mod.HAS_DISHKA, bool)


# ---------------------------------------------------------------------------
# Stubs accessible when dishka present (module-level symbols are importable)
# ---------------------------------------------------------------------------


def test__deps_module__provider_symbol__is_importable():
    # Arrange / Act / Assert — should not raise
    from aiokafka_foundation_kit.contrib.di._deps import Provider  # noqa: PLC0415

    assert Provider is not None


def test__deps_module__scope_symbol__is_importable():
    # Arrange / Act / Assert
    from aiokafka_foundation_kit.contrib.di._deps import Scope  # noqa: PLC0415

    assert Scope is not None


def test__deps_module__provide_symbol__is_importable():
    # Arrange / Act / Assert
    from aiokafka_foundation_kit.contrib.di._deps import provide  # noqa: PLC0415

    assert provide is not None
