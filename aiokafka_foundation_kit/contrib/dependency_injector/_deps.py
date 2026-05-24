"""Shared dependency-injector dependency helpers.

Centralizes the dependency-injector import boilerplate and availability check
so each module doesn't have to repeat it. Stub objects are exposed when
``dependency-injector`` is not installed so that container classes can always be
defined at import time and the missing dependency surfaces only on
instantiation via :func:`check_dependency_injector`.
"""

from __future__ import annotations

from typing import Any

from .._optional_deps import check_optional_dependency

try:
    import dependency_injector.containers as _dep_containers
    import dependency_injector.providers as _dep_providers

    containers = _dep_containers
    providers = _dep_providers
    HAS_DEPENDENCY_INJECTOR = True
except ImportError:  # pragma: no cover
    HAS_DEPENDENCY_INJECTOR = False  # pragma: no cover

    class _DeclarativeContainerStub:  # pragma: no cover
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            check_dependency_injector()

    class _ProvidersStub:  # pragma: no cover
        @staticmethod
        def Dependency(*args: Any, **kwargs: Any) -> Any:
            return None

        @staticmethod
        def Object(*args: Any, **kwargs: Any) -> Any:
            return None

        @staticmethod
        def Resource(*args: Any, **kwargs: Any) -> Any:
            return None

    class _ContainersStub:  # pragma: no cover
        DeclarativeContainer = _DeclarativeContainerStub

    containers = _ContainersStub()  # type: ignore[assignment]  # pragma: no cover
    providers = _ProvidersStub()  # type: ignore[assignment]  # pragma: no cover


def check_dependency_injector() -> None:
    """Raise :class:`ImportError` if ``dependency-injector`` is not installed."""
    check_optional_dependency(HAS_DEPENDENCY_INJECTOR, "dependency-injector", "for containers", "dependency-injector")
