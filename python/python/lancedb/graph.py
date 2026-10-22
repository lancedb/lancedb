# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Property graphs: graphs defined over tables a database already holds.

A property graph stores no rows. Its definition says which tables of its
namespace hold nodes and which hold edges: each node table's key and label
(:class:`NodeTable`), and each edge table's label and the node keys its source
and destination columns reference (:class:`EdgeTable`) -- the shape of
SQL/PGQ's ``CREATE PROPERTY GRAPH``. A query reads the tables through SQL
``GRAPH_TABLE``, as they are when it runs.

See ``DBConnection.create_property_graph``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from .background_loop import LOOP

if TYPE_CHECKING:
    from ._lancedb import Connection as LanceDbConnection


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
    created graph reports the list it resolved to, the key among them."""


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
    created graph reports the list it resolved to, both endpoint columns among
    them."""


@dataclass(frozen=True)
class PropertyGraphDescription:
    """What a database records about one property graph."""

    name: str
    """The graph's name within its namespace."""
    namespace_path: List[str]
    """The namespace holding the graph; empty is the root namespace."""
    nodes: List[NodeTable]
    """The node tables, each with the properties create resolved."""
    edges: List[EdgeTable]
    """The edge tables, each with the properties create resolved."""


def _definition_json(nodes: Sequence[NodeTable], edges: Sequence[EdgeTable]) -> str:
    """The definition a create request carries, refusing elements of the wrong
    type before any request is made."""
    nodes, edges = list(nodes), list(edges)
    for element, expected in [
        *((node, NodeTable) for node in nodes),
        *((edge, EdgeTable) for edge in edges),
    ]:
        if not isinstance(element, expected):
            raise TypeError(
                "A property graph's nodes are NodeTable and its edges EdgeTable, "
                f"not {type(element).__name__}"
            )
    return json.dumps(
        {
            "nodes": [_node_table_to_json(node) for node in nodes],
            "edges": [_edge_table_to_json(edge) for edge in edges],
        }
    )


def _description_from_json(text: str) -> PropertyGraphDescription:
    described = json.loads(text)
    return PropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        nodes=[_node_table_from_json(node) for node in described["nodes"]],
        edges=[_edge_table_from_json(edge) for edge in described.get("edges", [])],
    )


def _properties_to_json(
    encoded: Dict[str, Any], properties: Optional[List[str]]
) -> None:
    # Absent means every column, which an empty list would not.
    if properties is None:
        return
    if isinstance(properties, str):
        raise TypeError("properties is a list of column names, not a string")
    encoded["properties"] = list(properties)


def _node_table_to_json(node: NodeTable) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "table": node.table,
        "key": node.key,
        "label": node.label,
    }
    _properties_to_json(encoded, node.properties)
    return encoded


def _edge_table_to_json(edge: EdgeTable) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "table": edge.table,
        "label": edge.label,
        "source": _endpoint_to_json(edge.source),
        "destination": _endpoint_to_json(edge.destination),
    }
    _properties_to_json(encoded, edge.properties)
    return encoded


def _endpoint_to_json(endpoint: Endpoint) -> Dict[str, Any]:
    if not isinstance(endpoint, Endpoint):
        raise TypeError(
            "An edge's source and destination are Endpoint, "
            f"not {type(endpoint).__name__}"
        )
    table, column = endpoint.references
    return {
        "column": endpoint.column,
        "references": {"table": table, "column": column},
    }


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


class AsyncPropertyGraph:
    """A handle on one property graph: its name and namespace, and the
    connection it is described through.

    Obtained from ``AsyncConnection.create_property_graph`` or
    ``AsyncConnection.open_property_graph``. The graph is queried through SQL
    ``GRAPH_TABLE``, not through this handle.
    """

    def __init__(
        self,
        inner: "LanceDbConnection",
        name: str,
        namespace_path: Optional[List[str]] = None,
    ):
        self._inner = inner
        self._name = name
        self._namespace_path = list(namespace_path or [])

    def __repr__(self) -> str:
        return (
            f"AsyncPropertyGraph(name={self._name!r}, "
            f"namespace_path={self._namespace_path!r})"
        )

    @property
    def name(self) -> str:
        """The graph's name within its namespace."""
        return self._name

    @property
    def namespace_path(self) -> List[str]:
        """The namespace holding the graph; empty is the root namespace."""
        return list(self._namespace_path)

    async def describe(self) -> PropertyGraphDescription:
        """The graph's node and edge tables, each with the properties create
        resolved."""
        return _description_from_json(
            await self._inner.describe_property_graph(self._name, self._namespace_path)
        )


class PropertyGraph:
    """Synchronous variant of
    [AsyncPropertyGraph][lancedb.graph.AsyncPropertyGraph]."""

    def __init__(self, graph: AsyncPropertyGraph):
        self._async = graph

    def __repr__(self) -> str:
        return (
            f"PropertyGraph(name={self.name!r}, namespace_path={self.namespace_path!r})"
        )

    @property
    def name(self) -> str:
        """The graph's name within its namespace."""
        return self._async.name

    @property
    def namespace_path(self) -> List[str]:
        """The namespace holding the graph; empty is the root namespace."""
        return self._async.namespace_path

    def describe(self) -> PropertyGraphDescription:
        """The graph's node and edge tables, each with the properties create
        resolved."""
        return LOOP.run(self._async.describe())
