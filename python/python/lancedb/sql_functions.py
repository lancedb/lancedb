# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Common aggregate expressions for the DataFrame API."""

from ._lancedb import (
    aggregate_avg,
    aggregate_count,
    aggregate_max,
    aggregate_min,
    aggregate_sum,
)
from .expr import Expr


def sum(expression: Expr) -> Expr:
    """Sum non-null values."""
    return Expr(aggregate_sum(expression._inner))


def avg(expression: Expr) -> Expr:
    """Average non-null values."""
    return Expr(aggregate_avg(expression._inner))


def min(expression: Expr) -> Expr:
    """Return the minimum non-null value."""
    return Expr(aggregate_min(expression._inner))


def max(expression: Expr) -> Expr:
    """Return the maximum non-null value."""
    return Expr(aggregate_max(expression._inner))


def count(expression: Expr) -> Expr:
    """Count non-null values."""
    return Expr(aggregate_count(expression._inner))


__all__ = ["avg", "count", "max", "min", "sum"]
