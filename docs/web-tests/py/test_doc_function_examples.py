# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from lancedb.index import FTS


def test_python_boolean_query_operators(tmp_db):
    table = tmp_db.create_table(
        "boolean_docs",
        [
            {"id": 1, "text": "puppy"},
            {"id": 2, "text": "merrily"},
            {"id": 3, "text": "puppy merrily"},
            {"id": 4, "text": "other"},
        ],
    )
    table.create_index("text", config=FTS())

    # --8<-- [start:fts_boolean_queries]
    from lancedb.query import MatchQuery

    intersection = MatchQuery("puppy", "text") & MatchQuery("merrily", "text")
    union = MatchQuery("puppy", "text") | MatchQuery("merrily", "text")
    # --8<-- [end:fts_boolean_queries]

    def ids(query):
        return set(table.search(query, query_type="fts").to_arrow()["id"].to_pylist())

    assert ids(intersection) == {3}
    assert ids(union) == {1, 2, 3}
    a = MatchQuery("puppy", "text")
    b = MatchQuery("merrily", "text")
    assert ids(a and b) == {2, 3}
    assert ids(a or b) == {1, 3}
