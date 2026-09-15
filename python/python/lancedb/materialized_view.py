# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Materialized views: tables defined by a query over a source table and
maintained by refresh. See ``DBConnection.create_materialized_view``."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple, Union

from .background_loop import LOOP

if TYPE_CHECKING:
    import pyarrow as pa

    from ._lancedb import RefreshMaterializedViewResult
    from .table import AsyncTable, LanceTable

DEFINITION_META_KEY = b"mv.definition"

SelectArg = Union[
    str,
    Sequence[Union[str, Tuple[str, str]]],
    Dict[str, str],
    None,
]


DEFINITION_FORMAT = 1
"""The stored layout this version reads: ``{"format": 1, "query": "<SQL>"}``.
A ``kind`` key beside it is for readers older than the format number."""


@dataclass
class MaterializedViewDefinition:
    """The query that defines a materialized view, as stored::

    SELECT columns FROM [ns.]table [, UNNEST(column) AS alias]
      [WHERE predicate] [LIMIT n]
    """

    query: str
    """The defining query, in the canonical spelling the server stores."""


def _definition_from_schema(
    schema: "pa.Schema", name: str
) -> MaterializedViewDefinition:
    metadata = schema.metadata or {}
    raw = metadata.get(DEFINITION_META_KEY)
    if raw is None:
        raise ValueError(f"Table '{name}' is not a materialized view")
    value = json.loads(raw)
    fmt = value.get("format")
    if fmt is not None:
        # A newer writer's layout is reported, never guessed at.
        if not isinstance(fmt, int) or fmt > DEFINITION_FORMAT:
            raise NotImplementedError(
                f"materialized view '{name}' is stored in format {fmt}, which "
                "this version of lancedb cannot refresh"
            )
        return MaterializedViewDefinition(query=value["query"])
    # The structured layout written before the format number.
    kind = value.get("kind")
    if kind not in ("select", "namespaced_select"):
        raise NotImplementedError(
            f"materialized view '{name}' is stored in format kind '{kind}', "
            "which this version of lancedb cannot refresh"
        )
    return MaterializedViewDefinition(query=_legacy_query(value))


def _legacy_ident(name: str) -> str:
    if name and all(c == "_" or c.islower() or c.isdigit() for c in name):
        return name
    return _quote_identifier(name)


def _legacy_query(value: dict) -> str:
    """Render the pre-format structured layout as the query it described."""
    projections = value.get("projections", [])
    if projections:
        items = []
        for p in projections:
            output, expression = p["output"], p["expression"]
            if expression in (output, _quote_identifier(output)):
                items.append(expression)
            else:
                items.append(f"{expression} AS {_legacy_ident(output)}")
        columns = ", ".join(items)
    else:
        columns = "*"
    table = ".".join(
        _legacy_ident(part)
        for part in [*value.get("source_namespace", []), value["source_table"]]
    )
    query = f"SELECT {columns} FROM {table}"
    if value.get("filter") is not None:
        query += f" WHERE {value['filter']}"
    if value.get("limit") is not None:
        query += f" LIMIT {value['limit']}"
    return query


def _quote_identifier(name: str) -> str:
    """Quote a column name as a Lance SQL identifier (backticks)."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def normalize_select(select: SelectArg) -> Optional[List[Tuple[str, str]]]:
    """``select`` items may be a column name, an ``(alias, expression)`` pair,
    or a dict of the same. A bare name projects itself and is quoted, so any
    valid column name works; dict and pair entries are kept verbatim because
    their right side is an expression.

    A lone string is one column, not a sequence of its characters."""
    if select is None:
        return None
    if isinstance(select, str):
        select = [select]
    if isinstance(select, dict):
        return list(select.items())
    normalized = []
    for item in select:
        if isinstance(item, str):
            normalized.append((item, _quote_identifier(item)))
        else:
            alias, expression = item
            normalized.append((alias, expression))
    return normalized


class AsyncMaterializedView:
    """A handle on a materialized view: its table plus its definition.

    Obtained from ``AsyncConnection.create_materialized_view`` or
    ``AsyncConnection.open_materialized_view``.
    """

    def __init__(self, table: "AsyncTable"):
        self._table = table

    def __repr__(self) -> str:
        return f"AsyncMaterializedView(name={self.name!r})"

    @property
    def name(self) -> str:
        return self._table.name

    @property
    def table(self) -> "AsyncTable":
        """The view, as the table it is. Queries, indexes and search all
        apply; writes are not blocked, but a rebuild replaces them."""
        return self._table

    async def definition(self) -> MaterializedViewDefinition:
        """The query that defines the view, read from its stored schema."""
        return _definition_from_schema(await self._table.schema(), self.name)

    async def refresh(
        self, *, full: bool = False, source_version: Optional[int] = None
    ) -> "RefreshMaterializedViewResult":
        """Recompute the view from its source.

        The refresh is incremental when the source's changes can be
        reconciled into the view -- rows added, changed or removed since the
        last one -- and otherwise rebuilds. ``full=True`` forces a rebuild;
        ``source_version`` refreshes to that source version instead of the
        latest.

        Concurrent refreshes of one view do not duplicate its rows. Two that
        plan the same source rows conflict on commit, and the loser raises
        rather than writing them a second time.
        """
        return await self._table._inner.refresh_materialized_view(
            full=full, source_version=source_version
        )


class MaterializedView:
    """Synchronous variant of
    [AsyncMaterializedView][lancedb.materialized_view.AsyncMaterializedView]."""

    def __init__(self, table: "LanceTable"):
        self._table = table
        self._async = AsyncMaterializedView(table._table)

    def __repr__(self) -> str:
        return f"MaterializedView(name={self.name!r})"

    @property
    def name(self) -> str:
        return self._table.name

    @property
    def table(self) -> "LanceTable":
        """The view, as the table it is."""
        return self._table

    @property
    def definition(self) -> MaterializedViewDefinition:
        """The query that defines the view, read from its stored schema."""
        return _definition_from_schema(self._table.schema, self.name)

    def refresh(
        self, *, full: bool = False, source_version: Optional[int] = None
    ) -> "RefreshMaterializedViewResult":
        """Recompute the view from its source. See
        [AsyncMaterializedView.refresh][lancedb.materialized_view.AsyncMaterializedView.refresh]."""
        return LOOP.run(self._async.refresh(full=full, source_version=source_version))
