# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Client-side model of derived-compute lineage.

`Connection.lineage()` / `Table.lineage()` / `MaterializedView.lineage()` return
a `Lineage`: the graph of what a column or materialized view derives from
(upstream), what derives from it (downstream), and -- for each derived column --
the function that produced it, the version it was produced with, and whether
that is stale relative to the function the registry now holds.

The server returns this as JSON (the wire contract); these classes deserialize
it. Nothing here talks to the server.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import List, Optional, Union


@dataclass
class FunctionRef:
    """The function that produced a derived column, with version + location."""

    name: str
    #: Version that produced the data (stamped at compute time), if known.
    as_computed_version: Optional[str] = None
    #: Version the registry currently holds for this function name.
    current_version: Optional[str] = None
    #: True when the column was produced by an older function than the registry
    #: now holds -- i.e. silently stale; re-refresh to catch up.
    stale_vs_current: bool = False
    language: Optional[str] = None
    docker_image: Optional[str] = None
    env_digest: Optional[str] = None
    code_uri: Optional[str] = None

    @classmethod
    def _from(cls, d: dict) -> "FunctionRef":
        return cls(
            name=d["name"],
            as_computed_version=d.get("as_computed_version"),
            current_version=d.get("current_version"),
            stale_vs_current=d.get("stale_vs_current", False),
            language=d.get("language"),
            docker_image=d.get("docker_image"),
            env_digest=d.get("env_digest"),
            code_uri=d.get("code_uri"),
        )


@dataclass
class Node:
    """A lineage node: a table, view, column, or function."""

    kind: str  # "table" | "view" | "column" | "function"
    id: str  # "table", "table.column", or "fn:name@version"
    table: Optional[str] = None
    function: Optional[FunctionRef] = None

    @classmethod
    def _from(cls, d: dict) -> "Node":
        fn = d.get("function")
        return cls(
            kind=d["kind"],
            id=d["id"],
            table=d.get("table"),
            function=FunctionRef._from(fn) if fn else None,
        )


@dataclass
class Edge:
    """`downstream` depends on `upstream`, produced by `via` (a function name,
    or None for a passthrough)."""

    downstream: str
    upstream: str
    via: Optional[str] = None

    @classmethod
    def _from(cls, d: dict) -> "Edge":
        return cls(downstream=d["downstream"], upstream=d["upstream"], via=d.get("via"))


@dataclass
class Lineage:
    """A derived-compute lineage graph (nodes + labeled edges)."""

    target: str
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)

    @classmethod
    def from_json(cls, raw: Union[str, bytes, dict]) -> "Lineage":
        d = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        return cls(
            target=d.get("target", ""),
            nodes=[Node._from(n) for n in d.get("nodes", [])],
            edges=[Edge._from(e) for e in d.get("edges", [])],
        )

    def functions(self) -> List[FunctionRef]:
        """The function nodes in the graph."""
        return [n.function for n in self.nodes if n.function is not None]

    def stale(self) -> List[FunctionRef]:
        """Functions whose as-computed version is behind the current registry
        version -- the columns they produced are silently out of date."""
        return [f for f in self.functions() if f.stale_vs_current]

    def to_dict(self) -> dict:
        def prune(d: dict) -> dict:
            return {k: v for k, v in d.items() if v is not None}

        return {
            "target": self.target,
            "nodes": [
                prune(
                    {
                        "kind": n.kind,
                        "id": n.id,
                        "table": n.table,
                        "function": prune(vars(n.function)) if n.function else None,
                    }
                )
                for n in self.nodes
            ],
            "edges": [prune(vars(e)) for e in self.edges],
        }

    def to_graphviz(self) -> str:
        """Graphviz DOT for the lineage DAG: columns/tables as nodes, function
        names on edges, drift edges dashed + red."""
        stale_names = {f.name for f in self.stale()}
        out = [
            "digraph lineage {",
            "  rankdir=LR;",
            '  node [fontname="monospace"];',
        ]
        for n in self.nodes:
            if n.kind == "function":
                continue
            shape = "ellipse" if n.kind in ("table", "view") else "box"
            out.append(f'  "{n.id}" [shape={shape}];')
        for e in self.edges:
            attrs = ""
            if e.via:
                if e.via in stale_names:
                    attrs = f' [label="{e.via}" color=red style=dashed]'
                else:
                    attrs = f' [label="{e.via}"]'
            out.append(f'  "{e.upstream}" -> "{e.downstream}"{attrs};')
        out.append("}")
        return "\n".join(out)

    def _repr_html_(self) -> str:
        warn = ""
        drift = self.stale()
        if drift:
            names = ", ".join(sorted({f.name for f in drift}))
            warn = (
                f'<p style="color:#b00000"><b>stale vs current:</b> {names} '
                "(re-refresh to catch up)</p>"
            )
        rows = "".join(
            f"<tr><td><code>{e.downstream}</code></td>"
            f"<td>&larr; {e.via or ''}</td>"
            f"<td><code>{e.upstream}</code></td></tr>"
            for e in self.edges
        )
        return (
            f"<b>lineage: <code>{self.target}</code></b>{warn}"
            "<table><tr><th>derived</th><th>via</th><th>from</th></tr>"
            f"{rows}</table>"
        )
