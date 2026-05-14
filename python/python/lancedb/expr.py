# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Type-safe expression builder for filters and projections.

Instead of writing raw SQL strings you can build expressions with Python
operators::

    from lancedb.expr import col, lit

    # filter: age > 18 AND status = 'active'
    filt = (col("age") > lit(18)) & (col("status") == lit("active"))

    # projection: compute a derived column
    proj = {"score": col("raw_score") * lit(1.5)}

    table.search().where(filt).select(proj).to_list()
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Union

import pyarrow as pa

from lancedb._lancedb import PyExpr, expr_col, expr_lit, expr_func

__all__ = ["Expr", "col", "lit", "func"]

_STR_TO_PA_TYPE: dict = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float": pa.float32(),
    "float64": pa.float64(),
    "double": pa.float64(),
    "string": pa.string(),
    "utf8": pa.string(),
    "str": pa.string(),
    "large_string": pa.large_utf8(),
    "large_utf8": pa.large_utf8(),
    "date32": pa.date32(),
    "date": pa.date32(),
    "date64": pa.date64(),
}


def _coerce(value: "ExprLike") -> "Expr":
    """Return *value* as an :class:`Expr`, wrapping plain Python values via
    :func:`lit` if needed."""
    if isinstance(value, Expr):
        return value
    return lit(value)


# Type alias used in annotations.
ExprLike = Union["Expr", bool, int, float, str, bytes, date, datetime, Decimal]


class Expr:
    """A type-safe expression node.

    Construct instances with :func:`col` and :func:`lit`, then combine them
    using Python operators or the named methods below.

    Examples
    --------
    >>> from lancedb.expr import col, lit
    >>> filt = (col("age") > lit(18)) & (col("name").lower() == lit("alice"))
    >>> proj = {"double": col("x") * lit(2)}
    """

    # Make Expr unhashable so that == returns an Expr rather than being used
    # for dict keys / set membership.
    __hash__ = None  # type: ignore[assignment]

    def __init__(self, inner: PyExpr) -> None:
        self._inner = inner

    # ── comparisons ──────────────────────────────────────────────────────────

    def __eq__(self, other: ExprLike) -> "Expr":  # type: ignore[override]
        """Equal to (``col("x") == 1``)."""
        return Expr(self._inner.eq(_coerce(other)._inner))

    def __ne__(self, other: ExprLike) -> "Expr":  # type: ignore[override]
        """Not equal to (``col("x") != 1``)."""
        return Expr(self._inner.ne(_coerce(other)._inner))

    def __lt__(self, other: ExprLike) -> "Expr":
        """Less than (``col("x") < 1``)."""
        return Expr(self._inner.lt(_coerce(other)._inner))

    def __le__(self, other: ExprLike) -> "Expr":
        """Less than or equal to (``col("x") <= 1``)."""
        return Expr(self._inner.lte(_coerce(other)._inner))

    def __gt__(self, other: ExprLike) -> "Expr":
        """Greater than (``col("x") > 1``)."""
        return Expr(self._inner.gt(_coerce(other)._inner))

    def __ge__(self, other: ExprLike) -> "Expr":
        """Greater than or equal to (``col("x") >= 1``)."""
        return Expr(self._inner.gte(_coerce(other)._inner))

    # ── logical ──────────────────────────────────────────────────────────────

    def __and__(self, other: "Expr") -> "Expr":
        """Logical AND (``expr_a & expr_b``)."""
        return Expr(self._inner.and_(_coerce(other)._inner))

    def __rand__(self, other: ExprLike) -> "Expr":
        """Right-hand logical AND (``True & expr``)."""
        return Expr(_coerce(other)._inner.and_(self._inner))

    def __or__(self, other: "Expr") -> "Expr":
        """Logical OR (``expr_a | expr_b``)."""
        return Expr(self._inner.or_(_coerce(other)._inner))

    def __ror__(self, other: ExprLike) -> "Expr":
        """Right-hand logical OR (``False | expr``)."""
        return Expr(_coerce(other)._inner.or_(self._inner))

    def __invert__(self) -> "Expr":
        """Logical NOT (``~expr``)."""
        return Expr(self._inner.not_())

    # ── arithmetic ───────────────────────────────────────────────────────────

    def __add__(self, other: ExprLike) -> "Expr":
        """Add (``col("x") + 1``)."""
        return Expr(self._inner.add(_coerce(other)._inner))

    def __radd__(self, other: ExprLike) -> "Expr":
        """Right-hand add (``1 + col("x")``)."""
        return Expr(_coerce(other)._inner.add(self._inner))

    def __sub__(self, other: ExprLike) -> "Expr":
        """Subtract (``col("x") - 1``)."""
        return Expr(self._inner.sub(_coerce(other)._inner))

    def __rsub__(self, other: ExprLike) -> "Expr":
        """Right-hand subtract (``1 - col("x")``)."""
        return Expr(_coerce(other)._inner.sub(self._inner))

    def __mul__(self, other: ExprLike) -> "Expr":
        """Multiply (``col("x") * 2``)."""
        return Expr(self._inner.mul(_coerce(other)._inner))

    def __rmul__(self, other: ExprLike) -> "Expr":
        """Right-hand multiply (``2 * col("x")``)."""
        return Expr(_coerce(other)._inner.mul(self._inner))

    def __truediv__(self, other: ExprLike) -> "Expr":
        """Divide (``col("x") / 2``)."""
        return Expr(self._inner.div(_coerce(other)._inner))

    def __rtruediv__(self, other: ExprLike) -> "Expr":
        """Right-hand divide (``1 / col("x")``)."""
        return Expr(_coerce(other)._inner.div(self._inner))

    # ── string methods ───────────────────────────────────────────────────────

    def lower(self) -> "Expr":
        """Convert string column values to lowercase."""
        return Expr(self._inner.lower())

    def upper(self) -> "Expr":
        """Convert string column values to uppercase."""
        return Expr(self._inner.upper())

    def contains(self, substr: "ExprLike") -> "Expr":
        """Return True where the string contains *substr*."""
        return Expr(self._inner.contains(_coerce(substr)._inner))

    # ── type cast ────────────────────────────────────────────────────────────

    def cast(self, data_type: Union[str, "pa.DataType"]) -> "Expr":
        """Cast values to *data_type*.

        Parameters
        ----------
        data_type:
            A PyArrow ``DataType`` (e.g. ``pa.int32()``) or one of the type
            name strings: ``"bool"``, ``"int8"``, ``"int16"``, ``"int32"``,
            ``"int64"``, ``"uint8"``–``"uint64"``, ``"float32"``,
            ``"float64"``, ``"string"``, ``"date32"``, ``"date64"``.
        """
        if isinstance(data_type, str):
            try:
                data_type = _STR_TO_PA_TYPE[data_type]
            except KeyError:
                raise ValueError(
                    f"unsupported data type: '{data_type}'. Supported: "
                    f"{', '.join(_STR_TO_PA_TYPE)}"
                )
        return Expr(self._inner.cast(data_type))

    # ── named comparison helpers (alternative to operators) ──────────────────

    def eq(self, other: ExprLike) -> "Expr":
        """Equal to."""
        return self.__eq__(other)

    def ne(self, other: ExprLike) -> "Expr":
        """Not equal to."""
        return self.__ne__(other)

    def lt(self, other: ExprLike) -> "Expr":
        """Less than."""
        return self.__lt__(other)

    def lte(self, other: ExprLike) -> "Expr":
        """Less than or equal to."""
        return self.__le__(other)

    def gt(self, other: ExprLike) -> "Expr":
        """Greater than."""
        return self.__gt__(other)

    def gte(self, other: ExprLike) -> "Expr":
        """Greater than or equal to."""
        return self.__ge__(other)

    def and_(self, other: "Expr") -> "Expr":
        """Logical AND."""
        return self.__and__(other)

    def or_(self, other: "Expr") -> "Expr":
        """Logical OR."""
        return self.__or__(other)

    # ── utilities ────────────────────────────────────────────────────────────

    def to_sql(self) -> str:
        """Render the expression as a SQL string (useful for debugging)."""
        return self._inner.to_sql()

    def __repr__(self) -> str:
        return f"Expr({self._inner.to_sql()})"


# ── free functions ────────────────────────────────────────────────────────────


def col(name: str) -> Expr:
    """Reference a table column by name.

    Parameters
    ----------
    name:
        The column name.

    Examples
    --------
    >>> from lancedb.expr import col, lit
    >>> col("age") > lit(18)
    Expr((age > 18))
    """
    return Expr(expr_col(name))


def lit(value: Union[bool, int, float, str, bytes, date, datetime, Decimal]) -> Expr:
    """Create a literal (constant) value expression.

    Parameters
    ----------
    value:
        A Python ``bool``, ``int``, ``float``, ``str``, ``bytes``, ``date``,
        ``datetime``, or ``Decimal``.

    Examples
    --------
    >>> from lancedb.expr import col, lit
    >>> col("price") * lit(1.1)
    Expr((price * 1.1))
    """
    if not isinstance(value, (bool, int, float, str, bytes, date, datetime, Decimal)):
        raise TypeError(f"Unsupported literal type: {type(value).__name__}")

    return Expr(expr_lit(value))


def func(name: str, *args: ExprLike) -> Expr:
    """Call an arbitrary SQL function by name.

    Parameters
    ----------
    name:
        The SQL function name (e.g. ``"lower"``, ``"upper"``).
    *args:
        The function arguments as :class:`Expr` or plain Python literals.

    Examples
    --------
    >>> from lancedb.expr import col, func
    >>> func("lower", col("name"))
    Expr(lower(name))
    """
    inner_args = [_coerce(a)._inner for a in args]
    return Expr(expr_func(name, inner_args))
