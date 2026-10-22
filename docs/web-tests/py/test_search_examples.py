# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


def test_vector_search():
    # --8<-- [start:vector_search]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-search-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "documents",
            [
                {
                    "id": 1,
                    "text": "A puppy playing in the park",
                    "category": "animals",
                    "vector": [1.0, 0.0],
                },
                {
                    "id": 2,
                    "text": "A dog resting at home",
                    "category": "animals",
                    "vector": [0.9, 0.1],
                },
                {
                    "id": 3,
                    "text": "A guide to growing tomatoes",
                    "category": "gardening",
                    "vector": [0.0, 1.0],
                },
            ],
        )
        rows = (
            table.search([1.0, 0.0])
            .distance_type("l2")
            .select(["id", "text", "_distance"])
            .limit(2)
            .to_list()
        )
        print(rows)
    # --8<-- [end:vector_search]

    assert [row["id"] for row in rows] == [1, 2]
    assert rows[0]["_distance"] == 0.0


def test_text_search():
    # --8<-- [start:text_search]
    from tempfile import TemporaryDirectory

    import lancedb
    from lancedb.index import FTS

    with TemporaryDirectory(prefix="lancedb-search-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "documents",
            [
                {
                    "id": 1,
                    "text": "A puppy playing in the park",
                    "category": "animals",
                },
                {
                    "id": 2,
                    "text": "A dog resting at home",
                    "category": "animals",
                },
                {
                    "id": 3,
                    "text": "A guide to growing tomatoes",
                    "category": "gardening",
                },
            ],
        )
        table.create_index("text", config=FTS())
        rows = (
            table.search("puppy", query_type="fts", fts_columns="text")
            .select(["id", "text", "_score"])
            .limit(2)
            .to_list()
        )
        print(rows)
    # --8<-- [end:text_search]

    assert [row["id"] for row in rows] == [1]
    assert rows[0]["_score"] > 0


def test_hybrid_search():
    # --8<-- [start:hybrid_search]
    from tempfile import TemporaryDirectory

    import lancedb
    from lancedb.index import FTS

    with TemporaryDirectory(prefix="lancedb-search-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "documents",
            [
                {
                    "id": 1,
                    "text": "A puppy playing in the park",
                    "category": "animals",
                    "vector": [1.0, 0.0],
                },
                {
                    "id": 2,
                    "text": "A dog resting at home",
                    "category": "animals",
                    "vector": [0.9, 0.1],
                },
                {
                    "id": 3,
                    "text": "A guide to growing tomatoes",
                    "category": "gardening",
                    "vector": [0.0, 1.0],
                },
            ],
        )
        from lancedb.rerankers import RRFReranker

        table.create_index("text", config=FTS())
        rows = (
            table.search(
                query_type="hybrid", vector_column_name="vector", fts_columns="text"
            )
            .vector([1.0, 0.0])
            .text("puppy")
            .rerank(RRFReranker())
            .limit(2)
            .to_list()
        )
        print(rows)
    # --8<-- [end:hybrid_search]

    assert [row["id"] for row in rows] == [1, 2]
    assert rows[0]["_relevance_score"] > rows[1]["_relevance_score"]


def test_filter_search():
    # --8<-- [start:filter_search]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-search-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "documents",
            [
                {
                    "id": 1,
                    "text": "A puppy playing in the park",
                    "category": "animals",
                    "vector": [1.0, 0.0],
                },
                {
                    "id": 2,
                    "text": "A dog resting at home",
                    "category": "animals",
                    "vector": [0.9, 0.1],
                },
                {
                    "id": 3,
                    "text": "A guide to growing tomatoes",
                    "category": "gardening",
                    "vector": [0.0, 1.0],
                },
            ],
        )
        rows = (
            table.search([0.0, 1.0])
            .where("category = 'animals'", prefilter=True)
            .select(["id", "text", "category", "_distance"])
            .limit(2)
            .to_list()
        )
        print(rows)
    # --8<-- [end:filter_search]

    assert {row["id"] for row in rows} == {1, 2}
    assert all(row["category"] == "animals" for row in rows)


def test_query_plan():
    # --8<-- [start:query_plan]
    from tempfile import TemporaryDirectory

    import lancedb

    with TemporaryDirectory(prefix="lancedb-search-") as uri:
        db = lancedb.connect(uri)
        table = db.create_table(
            "documents",
            [
                {
                    "id": 1,
                    "text": "A puppy playing in the park",
                    "category": "animals",
                    "vector": [1.0, 0.0],
                },
                {
                    "id": 2,
                    "text": "A dog resting at home",
                    "category": "animals",
                    "vector": [0.9, 0.1],
                },
                {
                    "id": 3,
                    "text": "A guide to growing tomatoes",
                    "category": "gardening",
                    "vector": [0.0, 1.0],
                },
            ],
        )
        query = table.search([1.0, 0.0]).select(["id", "text", "_distance"]).limit(2)
        print("Plan:", query.explain_plan())
        print("Measured execution:", query.analyze_plan())
        rows = query.to_list()
        print(rows)
    # --8<-- [end:query_plan]

    assert [row["id"] for row in rows] == [1, 2]
