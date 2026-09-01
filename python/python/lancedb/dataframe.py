# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Lazy DataFrame plans for remote LanceDB execution."""

from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING, Any, Iterable, Optional, Sequence, Union

import pyarrow as pa

from ._lancedb import NativeDataFrame
from .expr import Expr, SortExpr, col
from .sql import AsyncQuery as AsyncSqlQuery
from .sql import Query as SqlQuery

if TYPE_CHECKING:
    from .arrow import AsyncRecordBatchReader

Expression = Union[str, Expr]


def _expression(value: Expression) -> Expr:
    if isinstance(value, Expr):
        return value
    if isinstance(value, str):
        return col(value)
    raise TypeError(f"expected a column name or Expr, got {type(value).__name__}")


class _DataFrameBase:
    def __init__(
        self,
        connection: Any,
        inner: NativeDataFrame,
        default_namespace_path: Sequence[str],
    ) -> None:
        self._connection = connection
        self._inner = inner
        self._default_namespace_path = list(default_namespace_path)

    def _wrap(self, inner: NativeDataFrame):
        return type(self)(self._connection, inner, self._default_namespace_path)

    @property
    def schema(self) -> pa.Schema:
        """The schema produced by the current logical plan."""
        return self._inner.schema()

    def select(self, *expressions: Expression):
        """Project columns or expressions."""
        if not expressions:
            raise ValueError("select requires at least one expression")
        return self._wrap(
            self._inner.select([_expression(value)._inner for value in expressions])
        )

    def filter(self, *predicates: Expr):
        """Keep rows matching every predicate."""
        if not predicates:
            raise ValueError("filter requires at least one predicate")
        predicate = reduce(lambda left, right: left & right, predicates)
        return self._wrap(self._inner.filter(predicate._inner))

    def aggregate(
        self,
        group_by: Optional[Union[Iterable[Expression], Expression]],
        aggregates: Union[Iterable[Expr], Expr],
    ):
        """Group rows and calculate aggregate expressions."""
        if group_by is None:
            group_values = []
        elif isinstance(group_by, (str, Expr)):
            group_values = [group_by]
        else:
            group_values = list(group_by)
        aggregate_values = [aggregates] if isinstance(aggregates, Expr) else aggregates
        groups = [_expression(value)._inner for value in group_values]
        aggregate_exprs = [value._inner for value in aggregate_values]
        return self._wrap(self._inner.aggregate(groups, aggregate_exprs))

    def sort(self, *expressions: Union[Expression, SortExpr]):
        """Sort rows using DataFusion-style sort expressions."""
        sorts = []
        for value in expressions:
            if isinstance(value, SortExpr):
                nulls_first = (
                    value.nulls_first
                    if value.nulls_first is not None
                    else not value.ascending
                )
                sorts.append((value.expr._inner, value.ascending, nulls_first))
            else:
                sorts.append((_expression(value)._inner, True, False))
        return self._wrap(self._inner.sort(sorts))

    def limit(self, count: int, offset: int = 0):
        """Limit the result to ``count`` rows after an optional offset."""
        return self._wrap(self._inner.limit(count, offset))

    def distinct(self):
        """Remove duplicate rows."""
        return self._wrap(self._inner.distinct())

    def alias(self, name: str):
        """Assign a relation alias, for example before a self join."""
        return self._wrap(self._inner.alias(name))

    def col(self, name: str) -> Expr:
        """Return a column expression qualified to this DataFrame.

        Use this to disambiguate columns after joins. The name is matched as a
        literal field name, so dots in column names are not treated as relation
        separators.
        """
        return Expr(self._inner.column(name))

    def column(self, name: str) -> Expr:
        """Alias for :meth:`col`."""
        return self.col(name)

    def with_column(self, name: str, expression: Expr):
        """Add or replace a column."""
        return self._wrap(self._inner.with_column(name, expression._inner))

    def with_columns(self, **expressions: Expr):
        """Add or replace multiple named columns."""
        result = self
        for name, expression in expressions.items():
            result = result.with_column(name, expression)
        return result

    def drop(self, *columns: str):
        """Remove columns from the result."""
        return self._wrap(self._inner.drop(list(columns)))

    def with_column_renamed(self, old_name: str, new_name: str):
        """Rename a column."""
        return self._wrap(self._inner.with_column_renamed(old_name, new_name))

    def join(
        self,
        other: "_DataFrameBase",
        on: Optional[Union[str, Sequence[str]]] = None,
        how: str = "inner",
        *,
        left_on: Optional[Union[str, Sequence[str]]] = None,
        right_on: Optional[Union[str, Sequence[str]]] = None,
    ):
        """Join two plans from the same connection and default namespace."""
        if self._connection is not other._connection:
            raise ValueError("joined DataFrames must use the same connection")
        if self._default_namespace_path != other._default_namespace_path:
            raise ValueError("joined DataFrames must use the same default namespace")
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("use either on or left_on/right_on")
            left_on = right_on = on
        if left_on is None or right_on is None:
            raise ValueError("join requires on or both left_on and right_on")

        def keys(value: Union[str, Sequence[str]]) -> list[str]:
            return [value] if isinstance(value, str) else list(value)

        return self._wrap(
            self._inner.join(other._inner, keys(left_on), keys(right_on), how)
        )

    def union(self, other: "_DataFrameBase", distinct: bool = False):
        """Union two compatible plans."""
        self._validate_set_operation(other)
        return self._wrap(self._inner.union(other._inner, not distinct))

    def intersect(self, other: "_DataFrameBase", distinct: bool = False):
        """Intersect two compatible plans."""
        self._validate_set_operation(other)
        return self._wrap(self._inner.intersect(other._inner, not distinct))

    def except_all(self, other: "_DataFrameBase", distinct: bool = False):
        """Remove rows present in another compatible plan."""
        self._validate_set_operation(other)
        return self._wrap(self._inner.except_(other._inner, not distinct))

    def _validate_set_operation(self, other: "_DataFrameBase") -> None:
        if self._connection is not other._connection:
            raise ValueError("DataFrames must use the same connection")
        if self._default_namespace_path != other._default_namespace_path:
            raise ValueError("DataFrames must use the same default namespace")

    def to_substrait(self) -> bytes:
        """Serialize the current logical plan as a Substrait protobuf."""
        return self._inner.to_substrait()

    def _to_substrait_with_version(self) -> tuple[bytes, str]:
        return self._inner.to_substrait_with_version()

    def __repr__(self) -> str:
        return repr(self._inner)


class DataFrame(_DataFrameBase):
    """A lazy DataFusion-style plan submitted to a synchronous connection."""

    def execute(self) -> pa.RecordBatchReader:
        """Submit this plan and return a blocking Arrow reader."""
        plan, version = self._to_substrait_with_version()
        return self._connection.execute_substrait(
            plan,
            version=version,
            default_namespace_path=self._default_namespace_path,
        )

    def execute_async(self) -> SqlQuery:
        """Submit this plan and return its server-side query lifecycle handle."""
        plan, version = self._to_substrait_with_version()
        return self._connection.execute_substrait_async(
            plan,
            version=version,
            default_namespace_path=self._default_namespace_path,
        )


class AsyncDataFrame(_DataFrameBase):
    """A lazy DataFusion-style plan submitted to an asynchronous connection."""

    async def execute(self) -> "AsyncRecordBatchReader":
        """Submit this plan and return an asynchronous Arrow reader."""
        plan, version = self._to_substrait_with_version()
        return await self._connection.execute_substrait(
            plan,
            version=version,
            default_namespace_path=self._default_namespace_path,
        )

    async def execute_async(self) -> AsyncSqlQuery:
        """Submit this plan and return its server-side query lifecycle handle."""
        plan, version = self._to_substrait_with_version()
        return await self._connection.execute_substrait_async(
            plan,
            version=version,
            default_namespace_path=self._default_namespace_path,
        )


__all__ = ["AsyncDataFrame", "DataFrame"]
