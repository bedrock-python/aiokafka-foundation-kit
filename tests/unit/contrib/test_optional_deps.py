"""Unit tests for aiokafka_foundation_kit.contrib._optional_deps."""

from __future__ import annotations

import pytest

from aiokafka_foundation_kit.contrib._optional_deps import check_optional_dependency

# ---------------------------------------------------------------------------
# Installed — no exception
# ---------------------------------------------------------------------------


def test__check_optional_dependency__installed_true__does_not_raise():
    # Arrange / Act / Assert — no exception
    check_optional_dependency(True, "some-pkg", "for testing", "extra")


# ---------------------------------------------------------------------------
# Not installed — raises ImportError with correct message
# ---------------------------------------------------------------------------


def test__check_optional_dependency__installed_false__raises_import_error():
    # Arrange / Act / Assert
    with pytest.raises(ImportError):
        check_optional_dependency(False, "some-pkg", "for testing", "extra")


def test__check_optional_dependency__installed_false__message_contains_package_name():
    # Arrange / Act / Assert
    with pytest.raises(ImportError, match="some-pkg"):
        check_optional_dependency(False, "some-pkg", "for testing", "extra")


def test__check_optional_dependency__installed_false__message_contains_install_extra():
    # Arrange / Act / Assert
    with pytest.raises(ImportError, match="aiokafka-foundation-kit\\[extra\\]"):
        check_optional_dependency(False, "some-pkg", "for testing", "extra")


def test__check_optional_dependency__installed_false__message_contains_description():
    # Arrange / Act / Assert
    with pytest.raises(ImportError, match="for testing"):
        check_optional_dependency(False, "some-pkg", "for testing", "extra")


@pytest.mark.parametrize(
    "pkg,description,install_extra",
    [
        ("dishka", "for DI providers", "dishka"),
        ("dependency-injector", "for containers", "dependency-injector"),
        ("opentelemetry-instrumentation-aiokafka", "for telemetry", "telemetry"),
    ],
)
def test__check_optional_dependency__various_packages__error_message_formatted_correctly(
    pkg: str, description: str, install_extra: str
):
    # Arrange / Act / Assert
    with pytest.raises(ImportError) as exc_info:
        check_optional_dependency(False, pkg, description, install_extra)

    msg = str(exc_info.value)
    assert pkg in msg
    assert description in msg
    assert f"aiokafka-foundation-kit[{install_extra}]" in msg
