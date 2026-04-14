"""Unit tests for worker scope filtering used by catalog hashing."""

from __future__ import annotations

from catalog.worker import _should_exclude_scope


def test_should_exclude_system_scope() -> None:
    """_system scope must be excluded from catalog state input."""
    assert _should_exclude_scope("_system") is True


def test_should_not_exclude_user_scope() -> None:
    """User/application scopes must remain included."""
    assert _should_exclude_scope("inventory") is False
