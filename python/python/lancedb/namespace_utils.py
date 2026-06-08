# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Utility functions for namespace operations."""

from typing import Optional


_CREATE_NAMESPACE_MODES = frozenset({"create", "exist_ok", "overwrite"})
_DROP_NAMESPACE_MODES = frozenset({"SKIP", "FAIL"})
_DROP_NAMESPACE_BEHAVIORS = frozenset({"RESTRICT", "CASCADE"})


def _normalize_create_namespace_mode(mode: Optional[str]) -> Optional[str]:
    """Normalize create namespace mode to lowercase (API expects lowercase)."""
    if mode is None:
        return None
    normalized = mode.lower()
    if normalized not in _CREATE_NAMESPACE_MODES:
        raise ValueError(
            f"Invalid create namespace mode {mode!r}: "
            f"expected one of 'create', 'exist_ok', 'overwrite'"
        )
    return normalized


def _normalize_drop_namespace_mode(mode: Optional[str]) -> Optional[str]:
    """Normalize drop namespace mode to uppercase (API expects uppercase)."""
    if mode is None:
        return None
    normalized = mode.upper()
    if normalized not in _DROP_NAMESPACE_MODES:
        raise ValueError(
            f"Invalid drop namespace mode {mode!r}: expected one of 'skip', 'fail'"
        )
    return normalized


def _normalize_drop_namespace_behavior(behavior: Optional[str]) -> Optional[str]:
    """Normalize drop namespace behavior to uppercase (API expects uppercase)."""
    if behavior is None:
        return None
    normalized = behavior.upper()
    if normalized not in _DROP_NAMESPACE_BEHAVIORS:
        raise ValueError(
            f"Invalid drop namespace behavior {behavior!r}: "
            f"expected one of 'restrict', 'cascade'"
        )
    return normalized
