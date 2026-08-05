# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import assert_type

from lancedb.db import DBConnection
from lancedb.query import (
    LanceEmptyQueryBuilder,
    LanceFtsQueryBuilder,
    LanceHybridQueryBuilder,
    LanceVectorQueryBuilder,
)
from lancedb.table import LanceTable


def check_table_search_types(connection: DBConnection, lance_table: LanceTable) -> None:
    table = connection.open_table("table")

    assert_type(table.search(), LanceEmptyQueryBuilder)
    assert_type(table.search([1.0, 2.0]), LanceVectorQueryBuilder)
    assert_type(
        table.search("query"),
        LanceFtsQueryBuilder | LanceVectorQueryBuilder,
    )
    assert_type(table.search("query", query_type="vector"), LanceVectorQueryBuilder)
    assert_type(table.search("query", query_type="fts"), LanceFtsQueryBuilder)
    assert_type(table.search("query", query_type="hybrid"), LanceHybridQueryBuilder)
    assert_type(table.search("query", None, "vector"), LanceVectorQueryBuilder)
    assert_type(table.search("query", None, "fts"), LanceFtsQueryBuilder)
    assert_type(table.search("query", None, "hybrid"), LanceHybridQueryBuilder)

    assert_type(
        lance_table.search("query"),
        LanceFtsQueryBuilder | LanceVectorQueryBuilder,
    )
