# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Materialized views: tables defined by a query over a source table and
maintained by refresh. See ``DBConnection.create_materialized_view``."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
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


@dataclass
class MaterializedViewDefinition:
    """The query that defines a materialized view."""

    source_table: str
    """Name of the source table, in the same database as the view."""
    projections: List[Tuple[str, str]]
    """``(output column, SQL expression)`` pairs, in view schema order."""
    filter: Optional[str] = None
    """SQL predicate selecting the source rows the view holds."""
    limit: Optional[int] = None
    """Cap on the number of rows the view holds."""
    inputs: List[str] = field(default_factory=list)
    """Source columns the projections and filter read."""


def _definition_from_schema(
    schema: "pa.Schema", name: str
) -> MaterializedViewDefinition:
    metadata = schema.metadata or {}
    raw = metadata.get(DEFINITION_META_KEY)
    if raw is None:
        raise ValueError(f"Table '{name}' is not a materialized view")
    value = json.loads(raw)
    kind = value.get("kind")
    if kind != "select":
        raise NotImplementedError(
            f"materialized view '{name}' is defined by '{kind}', which this "
            "version of lancedb cannot refresh"
        )
    return MaterializedViewDefinition(
        source_table=value["source_table"],
        projections=[
            (p["output"], p["expression"]) for p in value.get("projections", [])
        ],
        filter=value.get("filter"),
        limit=value.get("limit"),
        inputs=value.get("inputs", []),
    )


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
