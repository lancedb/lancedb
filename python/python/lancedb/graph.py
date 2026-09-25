# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Property graphs: tables read as the nodes and edges of a graph.

A property graph is a catalog object in a namespace, next to the tables it
reads. Its definition says which tables hold nodes and which hold edges: each
node table's key and label, each edge table's label, and how its source and
destination columns reference node keys -- the shape of SQL/PGQ's
``CREATE PROPERTY GRAPH``. The tables stay ordinary tables. See
``DBConnection.create_property_graph``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


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
    """The columns exposed as node properties; every column when ``None``."""


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
    """The columns exposed as edge properties; every column when ``None``."""


@dataclass(frozen=True)
class PropertyGraphDescription:
    """What a database records about one property graph."""

    name: str
    """The graph's name within its namespace."""
    namespace_path: List[str]
    """The namespace holding the graph; empty is the root namespace."""
    nodes: List[NodeTable]
    """The node tables."""
    edges: List[EdgeTable]
    """The edge tables."""
    vertex_count: int
    """How many nodes the graph held when it was built."""
    edge_count: int
    """How many edges the graph held when it was built."""
    sources: Dict[str, int]
    """The version of each table the graph was built from, by table name."""


def _definition_json(nodes: Sequence[NodeTable], edges: Sequence[EdgeTable]) -> str:
    return json.dumps(
        {
            "nodes": [_node_to_json(node) for node in nodes],
            "edges": [_edge_to_json(edge) for edge in edges],
        }
    )


def _description_from_json(text: str) -> PropertyGraphDescription:
    described = json.loads(text)
    return PropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        nodes=[_node_from_json(node) for node in described["nodes"]],
        edges=[_edge_from_json(edge) for edge in described.get("edges", [])],
        vertex_count=described["vertex_count"],
        edge_count=described["edge_count"],
        sources={
            source["table"]: source["version"]
            for source in described.get("sources", [])
        },
    )


def _node_to_json(node: NodeTable) -> Dict[str, Any]:
    encoded: Dict[str, Any] = {
        "table": node.table,
        "key": node.key,
        "label": node.label,
    }
    if node.properties is not None:
        encoded["properties"] = list(node.properties)
    return encoded


def _edge_to_json(edge: EdgeTable) -> Dict[str, Any]:
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


def _node_from_json(node: Dict[str, Any]) -> NodeTable:
    return NodeTable(
        table=node["table"],
        key=node["key"],
        label=node["label"],
        properties=node.get("properties"),
    )


def _edge_from_json(edge: Dict[str, Any]) -> EdgeTable:
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
