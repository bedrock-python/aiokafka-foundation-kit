"""Unit tests for aiokafka_foundation_kit.contrib.dependency_injector._deps."""

from __future__ import annotations

import pytest

import aiokafka_foundation_kit.contrib.dependency_injector._deps as deps_mod
from aiokafka_foundation_kit.contrib.dependency_injector._deps import check_dependency_injector

# ---------------------------------------------------------------------------
# check_dependency_injector — installed
# ---------------------------------------------------------------------------


def test__check_dependency_injector__installed__does_not_raise(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DEPENDENCY_INJECTOR", True)

    # Act / Assert — no exception
    check_dependency_injector()


# ---------------------------------------------------------------------------
# check_dependency_injector — not installed
# ---------------------------------------------------------------------------


def test__check_dependency_injector__not_installed__raises_import_error(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DEPENDENCY_INJECTOR", False)

    # Act / Assert
    with pytest.raises(ImportError):
        check_dependency_injector()


def test__check_dependency_injector__not_installed__message_contains_dependency_injector(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DEPENDENCY_INJECTOR", False)

    # Act / Assert
    with pytest.raises(ImportError, match="dependency-injector"):
        check_dependency_injector()


def test__check_dependency_injector__not_installed__message_contains_install_hint(monkeypatch):
    # Arrange
    monkeypatch.setattr(deps_mod, "HAS_DEPENDENCY_INJECTOR", False)

    # Act / Assert
    with pytest.raises(ImportError, match="aiokafka-foundation-kit\\[dependency-injector\\]"):
        check_dependency_injector()


# ---------------------------------------------------------------------------
# HAS_DEPENDENCY_INJECTOR flag is a bool
# ---------------------------------------------------------------------------


def test__deps_module__has_dependency_injector_flag__is_bool():
    # Arrange / Act / Assert
    assert isinstance(deps_mod.HAS_DEPENDENCY_INJECTOR, bool)


# ---------------------------------------------------------------------------
# Stub symbols accessible at import time regardless of installed state
# ---------------------------------------------------------------------------


def test__deps_module__containers_symbol__is_importable():
    # Arrange / Act / Assert
    from aiokafka_foundation_kit.contrib.dependency_injector._deps import containers  # noqa: PLC0415

    assert containers is not None


def test__deps_module__providers_symbol__is_importable():
    # Arrange / Act / Assert
    from aiokafka_foundation_kit.contrib.dependency_injector._deps import providers  # noqa: PLC0415

    assert providers is not None
