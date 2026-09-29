# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Property graphs: catalog objects read as graphs, in three kinds that share
one namespace of names.

A *property graph* holds its own rows. Its node types (:class:`NodeType`) and
edge types (:class:`EdgeType`) have schemas, and rows are inserted into the
graph itself with :meth:`PropertyGraph.insert`.

A *virtual property graph* reads tables in its namespace. Its definition says
which tables hold nodes and which hold edges: each node table's key and label
(:class:`NodeTable`), each edge table's label and how its source and
destination columns reference node keys (:class:`EdgeTable`) -- the shape of
SQL/PGQ's ``CREATE PROPERTY GRAPH``. A query reads the tables as they are.

A *materialized virtual property graph* has a virtual graph's definition, and
is read as its last refresh built it: after the tables change,
:meth:`MaterializedVirtualPropertyGraph.refresh` brings it up to date.

See ``DBConnection.create_property_graph``,
``DBConnection.create_virtual_property_graph`` and
``DBConnection.create_materialized_virtual_property_graph``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Type

import pyarrow as pa

from ._lancedb import graph_schema_from_json, graph_schema_to_json
from .background_loop import LOOP
from .scannable import to_scannable

if TYPE_CHECKING:
    from .common import DATA

PROPERTY_GRAPH = "property_graph"
VIRTUAL_PROPERTY_GRAPH = "virtual_property_graph"
MATERIALIZED_VIRTUAL_PROPERTY_GRAPH = "materialized_virtual_property_graph"


@dataclass(frozen=True)
class NodeTable:
    """A table whose rows are the nodes of one label."""

    table: str
    """The table, in the graph's namespace."""
    key: str
    """The column that identifies a node."""
    label: str
    """The label its nodes carry."""
    properties: Optional[List[str]] = None
    """The columns exposed as node properties; every column when ``None``. A
    created graph reports the list it resolved to."""


@dataclass(frozen=True)
class Endpoint:
    """One end of an edge: a column of the edge table and the node key it holds."""

    column: str
    """The edge table's column."""
    references: Tuple[str, str]
    """The node table and its key column, as ``(table, column)``."""


@dataclass(frozen=True)
class EdgeTable:
    """A table whose rows are the edges of one label."""

    table: str
    """The table, in the graph's namespace."""
    label: str
    """The label its edges carry."""
    source: Endpoint
    """The column naming each edge's source node."""
    destination: Endpoint
    """The column naming each edge's destination node."""
    properties: Optional[List[str]] = None
    """The columns exposed as edge properties; every column when ``None``. A
    created graph reports the list it resolved to."""


@dataclass(frozen=True)
class NodeType:
    """The nodes of one label of a property graph."""

    label: str
    """The label its nodes carry."""
    key: str
    """The field that identifies a node: a non-nullable integer, string or
    binary field of ``schema``."""
    schema: pa.Schema
    """Every property, the key among them."""


@dataclass(frozen=True)
class EdgeType:
    """The edges of one label of a property graph."""

    label: str
    """The label its edges carry."""
    source: Tuple[str, str]
    """The label of each edge's source node, and the column inserted rows carry
    its key in, as ``(label, column)``."""
    destination: Tuple[str, str]
    """The label of each edge's destination node, and the column inserted rows
    carry its key in, as ``(label, column)``."""
    schema: Optional[pa.Schema] = None
    """The edge's properties, when it has any."""


@dataclass(frozen=True)
class PropertyGraphDescription:
    """What a database records about one property graph."""

    name: str
    """The graph's name within its namespace."""
    namespace_path: List[str]
    """The namespace holding the graph; empty is the root namespace."""
    nodes: List[NodeType]
    """The node types."""
    edges: List[EdgeType]
    """The edge types."""
    commit: str
    """The graph's current commit."""
    committed_at: str
    """When the current commit landed, as RFC 3339."""
    previous_commit: Optional[str]
    """The commit a rollback returns to, if there is one."""
    vertex_count: int
    """How many nodes the current commit holds."""
    edge_count: int
    """How many edges the current commit holds."""


@dataclass(frozen=True)
class VirtualPropertyGraphDescription:
    """What a database records about one virtual property graph."""

    name: str
    """The graph's name within its namespace."""
    namespace_path: List[str]
    """The namespace holding the graph; empty is the root namespace."""
    nodes: List[NodeTable]
    """The node tables, each with the properties create resolved."""
    edges: List[EdgeTable]
    """The edge tables, each with the properties create resolved."""


@dataclass(frozen=True)
class MaterializedVirtualPropertyGraphDescription:
    """What a database records about one materialized virtual property graph."""

    name: str
    """The graph's name within its namespace."""
    namespace_path: List[str]
    """The namespace holding the graph; empty is the root namespace."""
    nodes: List[NodeTable]
    """The node tables, each with the properties create resolved."""
    edges: List[EdgeTable]
    """The edge tables, each with the properties create resolved."""
    commit: str
    """The last refresh's commit."""
    committed_at: str
    """When the last refresh landed, as RFC 3339."""
    vertex_count: int
    """How many nodes the last refresh built."""
    edge_count: int
    """How many edges the last refresh built."""
    sources: Dict[str, int]
    """The version of each table the last refresh read, by table name."""


def _require(
    kind: str,
    nodes: Sequence[Any],
    edges: Sequence[Any],
    node_type: Type,
    edge_type: Type,
) -> None:
    for element, expected in [
        *((node, node_type) for node in nodes),
        *((edge, edge_type) for edge in edges),
    ]:
        if not isinstance(element, expected):
            raise TypeError(
                f"A {kind}'s nodes are {node_type.__name__} and its edges "
                f"{edge_type.__name__}, not {type(element).__name__}"
            )


def _types_json(nodes: Sequence[NodeType], edges: Sequence[EdgeType]) -> str:
    _require("property graph", nodes, edges, NodeType, EdgeType)
    return json.dumps(
        {
            "nodes": [_node_type_to_json(node) for node in nodes],
            "edges": [_edge_type_to_json(edge) for edge in edges],
        }
    )


def _tables_json(nodes: Sequence[NodeTable], edges: Sequence[EdgeTable]) -> str:
    _require("virtual property graph", nodes, edges, NodeTable, EdgeTable)
    return json.dumps(
        {
            "nodes": [_node_table_to_json(node) for node in nodes],
            "edges": [_edge_table_to_json(edge) for edge in edges],
        }
    )


def _property_description(text: str) -> PropertyGraphDescription:
    described = json.loads(text)
    return PropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        nodes=[_node_type_from_json(node) for node in described["nodes"]],
        edges=[_edge_type_from_json(edge) for edge in described.get("edges", [])],
        commit=described["commit"],
        committed_at=described["committed_at"],
        previous_commit=described.get("previous_commit"),
        vertex_count=described["vertex_count"],
        edge_count=described["edge_count"],
    )


def _virtual_description(text: str) -> VirtualPropertyGraphDescription:
    described = json.loads(text)
    return VirtualPropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        nodes=[_node_table_from_json(node) for node in described["nodes"]],
        edges=[_edge_table_from_json(edge) for edge in described.get("edges", [])],
    )


def _materialized_description(text: str) -> MaterializedVirtualPropertyGraphDescription:
    described = json.loads(text)
    return MaterializedVirtualPropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        nodes=[_node_table_from_json(node) for node in described["nodes"]],
        edges=[_edge_table_from_json(edge) for edge in described.get("edges", [])],
        commit=described["commit"],
        committed_at=described["committed_at"],
        vertex_count=described["vertex_count"],
        edge_count=described["edge_count"],
        sources={
            source["table"]: source["version"]
            for source in described.get("sources", [])
        },
    )


def _schema_to_json(schema: pa.Schema) -> Dict[str, Any]:
    return json.loads(graph_schema_to_json(schema))


def _schema_from_json(schema: Dict[str, Any]) -> pa.Schema:
    return graph_schema_from_json(json.dumps(schema))


def _node_type_to_json(node: NodeType) -> Dict[str, Any]:
    return {
        "label": node.label,
        "key": node.key,
        "schema": _schema_to_json(node.schema),
    }


def _edge_type_to_json(edge: EdgeType) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "label": edge.label,
        "source": {"label": edge.source[0], "column": edge.source[1]},
        "destination": {"label": edge.destination[0], "column": edge.destination[1]},
    }
    if edge.schema is not None:
        encoded["schema"] = _schema_to_json(edge.schema)
    return encoded


def _node_table_to_json(node: NodeTable) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "table": node.table,
        "key": node.key,
        "label": node.label,
    }
    if node.properties is not None:
        encoded["properties"] = list(node.properties)
    return encoded


def _edge_table_to_json(edge: EdgeTable) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "table": edge.table,
        "label": edge.label,
        "source": _endpoint_to_json(edge.source),
        "destination": _endpoint_to_json(edge.destination),
    }
    if edge.properties is not None:
        encoded["properties"] = list(edge.properties)
    return encoded


def _endpoint_to_json(endpoint: Endpoint) -> Dict[str, Any]:
    table, column = endpoint.references
    return {
        "column": endpoint.column,
        "references": {"table": table, "column": column},
    }


def _node_type_from_json(node: Dict[str, Any]) -> NodeType:
    return NodeType(
        label=node["label"], key=node["key"], schema=_schema_from_json(node["schema"])
    )


def _edge_type_from_json(edge: Dict[str, Any]) -> EdgeType:
    schema = edge.get("schema")
    return EdgeType(
        label=edge["label"],
        source=(edge["source"]["label"], edge["source"]["column"]),
        destination=(edge["destination"]["label"], edge["destination"]["column"]),
        schema=None if schema is None else _schema_from_json(schema),
    )


def _node_table_from_json(node: Dict[str, Any]) -> NodeTable:
    return NodeTable(
        table=node["table"],
        key=node["key"],
        label=node["label"],
        properties=node.get("properties"),
    )


def _edge_table_from_json(edge: Dict[str, Any]) -> EdgeTable:
    return EdgeTable(
        table=edge["table"],
        label=edge["label"],
        source=_endpoint_from_json(edge["source"]),
        destination=_endpoint_from_json(edge["destination"]),
        properties=edge.get("properties"),
    )


def _endpoint_from_json(endpoint: Dict[str, Any]) -> Endpoint:
    references = endpoint["references"]
    return Endpoint(
        column=endpoint["column"],
        references=(references["table"], references["column"]),
    )


class _AsyncGraph:
    """A handle on one graph of a database: its name and namespace, and the
    connection it is read and written through."""

    _kind: str = PROPERTY_GRAPH

    def __init__(self, inner: Any, name: str, namespace_path: Optional[List[str]]):
        self._inner = inner
        self._name = name
        self._namespace_path = list(namespace_path or [])

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self._name!r})"

    @property
    def name(self) -> str:
        """The graph's name within its namespace."""
        return self._name

    @property
    def namespace_path(self) -> List[str]:
        """The namespace holding the graph; empty is the root namespace."""
        return list(self._namespace_path)

    async def _describe(self) -> str:
        return await self._inner.describe_graph(
            self._kind, self._name, self._namespace_path
        )


class AsyncPropertyGraph(_AsyncGraph):
    """A property graph: one that holds its own rows.

    Obtained from ``AsyncConnection.create_property_graph`` or
    ``AsyncConnection.open_property_graph``.
    """

    _kind = PROPERTY_GRAPH

    async def describe(self) -> PropertyGraphDescription:
        """The graph's node and edge types, current commit and size."""
        return _property_description(await self._describe())

    async def insert(self, label: str, data: "DATA") -> PropertyGraphDescription:
        """Insert rows of one node label or edge label, as one commit.

        Nodes upsert by key: a key the graph holds replaces that node's
        properties. Edges append, carrying their endpoints' keys in the columns
        the edge type names; an edge whose endpoint is not a node of the graph
        refuses the whole insert.
        """
        return _property_description(
            await self._inner.insert_into_property_graph(
                self._name, label, to_scannable(data), self._namespace_path
            )
        )

    async def rollback(self) -> PropertyGraphDescription:
        """Return the graph to its previous commit.

        The commit rolled back from is discarded, so a second rollback in a row
        is an error.
        """
        return _property_description(
            await self._inner.rollback_property_graph(self._name, self._namespace_path)
        )


class AsyncVirtualPropertyGraph(_AsyncGraph):
    """A virtual property graph: one defined over tables and read through them.

    Obtained from ``AsyncConnection.create_virtual_property_graph`` or
    ``AsyncConnection.open_virtual_property_graph``.
    """

    _kind = VIRTUAL_PROPERTY_GRAPH

    async def describe(self) -> VirtualPropertyGraphDescription:
        """The graph's node and edge tables."""
        return _virtual_description(await self._describe())


class AsyncMaterializedVirtualPropertyGraph(_AsyncGraph):
    """A materialized virtual property graph: one defined over tables and read
    as its last refresh built it.

    Obtained from ``AsyncConnection.create_materialized_virtual_property_graph``
    or ``AsyncConnection.open_materialized_virtual_property_graph``.
    """

    _kind = MATERIALIZED_VIRTUAL_PROPERTY_GRAPH

    async def describe(self) -> MaterializedVirtualPropertyGraphDescription:
        """The graph's node and edge tables, and what its last refresh built."""
        return _materialized_description(await self._describe())

    async def refresh(self) -> MaterializedVirtualPropertyGraphDescription:
        """Rebuild the graph from its tables' latest versions.

        When every table's commits since the last refresh left its rows as
        they were -- compaction, indexing, metadata -- only the recorded
        versions advance; when nothing changed, the commit stays.
        """
        return _materialized_description(
            await self._inner.refresh_materialized_virtual_property_graph(
                self._name, self._namespace_path
            )
        )


class _Graph:
    """Synchronous variant of a graph handle."""

    def __init__(self, graph: _AsyncGraph):
        self._async = graph

    def __repr__(self) -> str:
        return f"{type(self).__name__}(name={self.name!r})"

    @property
    def name(self) -> str:
        """The graph's name within its namespace."""
        return self._async.name

    @property
    def namespace_path(self) -> List[str]:
        """The namespace holding the graph; empty is the root namespace."""
        return self._async.namespace_path


class PropertyGraph(_Graph):
    """Synchronous variant of
    [AsyncPropertyGraph][lancedb.graph.AsyncPropertyGraph]."""

    _async: AsyncPropertyGraph

    def describe(self) -> PropertyGraphDescription:
        """The graph's node and edge types, current commit and size."""
        return LOOP.run(self._async.describe())

    def insert(self, label: str, data: "DATA") -> PropertyGraphDescription:
        """Insert rows of one node label or edge label, as one commit. See
        [AsyncPropertyGraph.insert][lancedb.graph.AsyncPropertyGraph.insert]."""
        return LOOP.run(self._async.insert(label, data))

    def rollback(self) -> PropertyGraphDescription:
        """Return the graph to its previous commit. See
        [AsyncPropertyGraph.rollback][lancedb.graph.AsyncPropertyGraph.rollback]."""
        return LOOP.run(self._async.rollback())


class VirtualPropertyGraph(_Graph):
    """Synchronous variant of
    [AsyncVirtualPropertyGraph][lancedb.graph.AsyncVirtualPropertyGraph]."""

    _async: AsyncVirtualPropertyGraph

    def describe(self) -> VirtualPropertyGraphDescription:
        """The graph's node and edge tables."""
        return LOOP.run(self._async.describe())


class MaterializedVirtualPropertyGraph(_Graph):
    """Synchronous variant of
    [AsyncMaterializedVirtualPropertyGraph][lancedb.graph.AsyncMaterializedVirtualPropertyGraph]."""

    _async: AsyncMaterializedVirtualPropertyGraph

    def describe(self) -> MaterializedVirtualPropertyGraphDescription:
        """The graph's node and edge tables, and what its last refresh built."""
        return LOOP.run(self._async.describe())

    def refresh(self) -> MaterializedVirtualPropertyGraphDescription:
        """Rebuild the graph from its tables' latest versions. See
        [AsyncMaterializedVirtualPropertyGraph.refresh][lancedb.graph.AsyncMaterializedVirtualPropertyGraph.refresh]."""
        return LOOP.run(self._async.refresh())
