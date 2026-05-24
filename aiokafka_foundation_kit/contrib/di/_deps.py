"""Shared dishka dependency helpers.

Centralizes the dishka import boilerplate and availability check so each
provider module doesn't have to repeat it. Stub objects are exposed when
``dishka`` is not installed so that classes can always be defined at import
time and the missing dependency surfaces only when the provider is actually
instantiated.
"""

from __future__ import annotations

from typing import Any

from .._optional_deps import check_optional_dependency

try:
    import dishka as _dishka

    Provider = _dishka.Provider
    Scope = _dishka.Scope
    provide = _dishka.provide
    HAS_DISHKA = True
except ImportError:  # pragma: no cover
    HAS_DISHKA = False  # pragma: no cover

    class _ScopeStub:  # pragma: no cover
        APP = None
        REQUEST = None

    class Provider:  # type: ignore[no-redef]  # pragma: no cover
        """Stub used when ``dishka`` is not installed.

        Real provider classes still inherit from this so that imports succeed;
        instantiation raises a clear :class:`ImportError` via ``__init_subclass__``.
        """

        scope: Any = None

    Scope = _ScopeStub()  # type: ignore[misc,assignment]  # pragma: no cover

    def provide(*args: Any, **kwargs: Any) -> Any:  # type: ignore[no-redef]  # pragma: no cover
        """No-op decorator used when ``dishka`` is not installed."""
        if args and callable(args[0]):
            return args[0]
        return lambda func: func


def check_dishka() -> None:
    """Raise :class:`ImportError` if ``dishka`` is not installed."""
    check_optional_dependency(HAS_DISHKA, "dishka", "for DI providers", "dishka")
