# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Property graphs: catalog objects read as graphs, in one of two modes.

An *independent* graph holds its own rows. Its node types (:class:`NodeType`)
and edge types (:class:`EdgeType`) have schemas, and rows are inserted into the
graph itself with ``insert_into_property_graph``.

A *materialized view* graph reads tables in its namespace. Its definition says
which tables hold nodes and which hold edges: each node table's key and label
(:class:`NodeTable`), each edge table's label and how its source and
destination columns reference node keys (:class:`EdgeTable`) -- the shape of
SQL/PGQ's ``CREATE PROPERTY GRAPH``. The tables stay ordinary tables; after
they change, ``refresh_property_graph`` brings the graph up to date.

See ``DBConnection.create_property_graph``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pyarrow as pa

from ._lancedb import graph_schema_from_json, graph_schema_to_json


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
    """The nodes of one label of an independent graph."""

    label: str
    """The label its nodes carry."""
    key: str
    """The field that identifies a node: a non-nullable integer, string or
    binary field of ``schema``."""
    schema: pa.Schema
    """Every property, the key among them."""


@dataclass(frozen=True)
class EdgeType:
    """The edges of one label of an independent graph."""

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
    mode: str
    """``"independent"`` or ``"materialized_view"``."""
    nodes: List[Union[NodeTable, NodeType]]
    """The node tables (materialized view) or node types (independent)."""
    edges: List[Union[EdgeTable, EdgeType]]
    """The edge tables (materialized view) or edge types (independent)."""
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
    sources: Dict[str, int]
    """For a materialized view, the version of each table the current commit
    was refreshed from, by table name."""


INDEPENDENT = "independent"
MATERIALIZED_VIEW = "materialized_view"


def _mode_of(nodes: Sequence[Any], edges: Sequence[Any]) -> str:
    kinds = {type(element) for element in [*nodes, *edges]}
    if kinds <= {NodeType, EdgeType} and kinds:
        return INDEPENDENT
    if kinds <= {NodeTable, EdgeTable}:
        return MATERIALIZED_VIEW
    raise ValueError(
        "A property graph's nodes and edges are either all tables "
        "(NodeTable, EdgeTable) or all types (NodeType, EdgeType)"
    )


def _definition_json(nodes: Sequence[Any], edges: Sequence[Any]) -> str:
    mode = _mode_of(nodes, edges)
    return json.dumps(
        {
            "mode": mode,
            "nodes": [_node_to_json(node) for node in nodes],
            "edges": [_edge_to_json(edge) for edge in edges],
        }
    )


def _description_from_json(text: str) -> PropertyGraphDescription:
    described = json.loads(text)
    mode = described["mode"]
    return PropertyGraphDescription(
        name=described["name"],
        namespace_path=list(described.get("namespace", [])),
        mode=mode,
        nodes=[_node_from_json(mode, node) for node in described["nodes"]],
        edges=[_edge_from_json(mode, edge) for edge in described.get("edges", [])],
        commit=described["commit"],
        committed_at=described["committed_at"],
        previous_commit=described.get("previous_commit"),
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


def _node_to_json(node: Union[NodeTable, NodeType]) -> Dict[str, Any]:
    if isinstance(node, NodeType):
        return {
            "label": node.label,
            "key": node.key,
            "schema": _schema_to_json(node.schema),
        }
    encoded: Dict[str, Any] = {
        "table": node.table,
        "key": node.key,
        "label": node.label,
    }
    if node.properties is not None:
        encoded["properties"] = list(node.properties)
    return encoded


def _edge_to_json(edge: Union[EdgeTable, EdgeType]) -> Dict[str, Any]:
    if isinstance(edge, EdgeType):
        encoded: Dict[str, Any] = {
            "label": edge.label,
            "source": {"label": edge.source[0], "column": edge.source[1]},
            "destination": {
                "label": edge.destination[0],
                "column": edge.destination[1],
            },
        }
        if edge.schema is not None:
            encoded["schema"] = _schema_to_json(edge.schema)
        return encoded
    encoded = {
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


def _node_from_json(mode: str, node: Dict[str, Any]) -> Union[NodeTable, NodeType]:
    if mode == INDEPENDENT:
        return NodeType(
            label=node["label"],
            key=node["key"],
            schema=_schema_from_json(node["schema"]),
        )
    return NodeTable(
        table=node["table"],
        key=node["key"],
        label=node["label"],
        properties=node.get("properties"),
    )


def _edge_from_json(mode: str, edge: Dict[str, Any]) -> Union[EdgeTable, EdgeType]:
    if mode == INDEPENDENT:
        schema = edge.get("schema")
        return EdgeType(
            label=edge["label"],
            source=(edge["source"]["label"], edge["source"]["column"]),
            destination=(edge["destination"]["label"], edge["destination"]["column"]),
            schema=None if schema is None else _schema_from_json(schema),
        )
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
