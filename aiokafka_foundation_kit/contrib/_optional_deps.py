"""Shared helper for optional dependency checks."""


def check_optional_dependency(installed: bool, pkg: str, description: str, install_extra: str) -> None:
    """Raise :class:`ImportError` if an optional dependency is not installed."""
    if not installed:
        raise ImportError(
            f"{pkg} is required {description}. Install with: pip install 'aiokafka-foundation-kit[{install_extra}]'"
        )
