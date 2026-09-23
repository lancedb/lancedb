# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Views: a named query a database stores and plans on every read.

A view holds no rows. What it stores is the statement that defines it and the
schema that statement resolved to, so a reader sees the sources as they are
now. See ``DBConnection.create_view``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class ViewDescription:
    """What a database records about one view."""

    name: str
    """The view's name within its namespace."""
    query: str
    """The defining query, as the database stores it."""
    default_database: str
    """The database that unqualified table names in ``query`` resolve against."""
    schema: "pa.Schema"
    """The schema the defining query resolved to when the view was created."""
    namespace_path: List[str] = field(default_factory=list)
    """The namespace holding the view; empty is the root namespace."""
    default_namespace_path: List[str] = field(default_factory=list)
    """The namespace path those unqualified names resolve against.

    Recorded with the view because it outlives the session that declared it: a
    reader resolving the query against its own default namespace could read a
    different table than the view was defined over.
    """
