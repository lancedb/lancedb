# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import random

import lancedb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from lancedb.conftest import MockTextEmbeddingFunction  # noqa
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import (
    LinearCombinationReranker,
    RRFReranker,
    CohereReranker,
    ColbertReranker,
    CrossEncoderReranker,
    OpenaiReranker,
    JinaReranker,
    AnswerdotaiRerankers,
    VoyageAIReranker,
    MRRReranker,
)
from lancedb.table import LanceTable


def get_test_table(tmp_path):
    db = lancedb.connect(tmp_path)
    # Create a LanceDB table schema with a vector and a text column
    emb = EmbeddingFunctionRegistry.get_instance().get("test").create()
    meta_emb = EmbeddingFunctionRegistry.get_instance().get("test").create()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()
        meta: str = meta_emb.SourceField()
        meta_vector: Vector(meta_emb.ndims()) = meta_emb.VectorField()

    # Initialize the table using the schema
    table = LanceTable.create(
        db,
        "my_table",
        schema=MyTable,
        mode="overwrite",
    )

    # Need to test with a bunch of phrases to make sure sorting is consistent
    phrases = [
        "great kid don't get cocky",
        "now that's a name I haven't heard in a long time",
        "if you strike me down I shall become more powerful than you imagine",
        "I find your lack of faith disturbing",
        "I've got a bad feeling about this",
        "never tell me the odds",
        "I am your father",
        "somebody has to save our skins",
        "New strategy R2 let the wookiee win",
        "Arrrrggghhhhhhh",
        "I see a mansard roof through the trees",
        "I see a salty message written in the eves",
        "the ground beneath my feet",
        "the hot garbage and concrete",
        "and now the tops of buildings",
        "everybody with a worried mind could never forgive the sight",
        "of wicked snakes inside a place you thought was dignified",
        "I don't wanna live like this",
        "but I don't wanna die",
        "The templars want control",
        "the brotherhood of assassins want freedom",
        "if only they could both see the world as it really is",
        "there would be peace",
        "but the war goes on",
        "altair's legacy was a warning",
        "Kratos had a son",
        "he was a god",
        "the god of war",
        "but his son was mortal",
        "there hasn't been a good battlefield game since 2142",
        "I wish they would make another one",
        "campains are not as good as they used to be",
        "Multiplayer and open world games have destroyed the single player experience",
        "Maybe the future is console games",
        "I don't know",
    ]

    # Add the phrases and vectors to the table
    table.add(
        [
            {"text": p, "meta": phrases[random.randint(0, len(phrases) - 1)]}
            for p in phrases
        ]
    )

    # Create a fts index
    table.create_fts_index("text", replace=True)

    return table, MyTable


def _run_test_reranker(reranker, table, query, query_vector, schema):
    # Hybrid search setting
    result1 = (
        table.search(query, query_type="hybrid", vector_column_name="vector")
        .rerank(normalize="score", reranker=reranker)
        .to_pydantic(schema)
    )
    result2 = (
        table.search(query, query_type="hybrid", vector_column_name="vector")
        .rerank(reranker=reranker)
        .to_pydantic(schema)
    )
    assert result1 == result2

    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search(query_type="hybrid", vector_column_name="vector")
        .vector(query_vector)
        .text(query)
        .limit(30)
        .rerank(reranker=reranker)
        .to_arrow()
    )

    assert len(result) == 30
    ascending_relevance_err = (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )

    # Vector search setting
    result = (
        table.search(query, vector_column_name="vector")
        .rerank(reranker=reranker)
        .limit(30)
        .to_arrow()
    )
    assert len(result) == 30
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )
    result_explicit = (
        table.search(query_vector, vector_column_name="vector")
        .rerank(reranker=reranker, query_string=query)
        .limit(30)
        .to_arrow()
    )
    assert len(result_explicit) == 30
    with pytest.raises(
        ValueError
    ):  # This raises an error because vector query is provided without reanking query
        table.search(query_vector, vector_column_name="vector").rerank(
            reranker=reranker
        ).limit(30).to_arrow()

    # FTS search setting
    result = (
        table.search(query, query_type="fts", vector_column_name="vector")
        .rerank(reranker=reranker)
        .limit(30)
        .to_arrow()
    )
    assert len(result) > 0
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )

    # empty FTS results
    query = "abcxyz" * 100
    result = (
        table.search(query_type="hybrid", vector_column_name="vector")
        .vector(query_vector)
        .text(query)
        .limit(30)
        .rerank(reranker=reranker)
        .to_arrow()
    )

    # should return _relevance_score column
    assert "_relevance_score" in result.column_names
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )

    # Multi-vector search setting
    rs1 = table.search(query, vector_column_name="vector").limit(10).with_row_id(True)
    rs2 = (
        table.search(query, vector_column_name="meta_vector")
        .limit(10)
        .with_row_id(True)
    )
    result = reranker.rerank_multivector([rs1, rs2], query)
    assert len(result) == 20
    result_deduped = reranker.rerank_multivector(
        [rs1, rs2, rs1], query, deduplicate=True
    )
    assert len(result_deduped) <= 20
    result_arrow = reranker.rerank_multivector([rs1.to_arrow(), rs2.to_arrow()], query)
    assert len(result) == 20 and result == result_arrow


def _run_test_hybrid_reranker(reranker, tmp_path):
    table, schema = get_test_table(tmp_path)
    # The default reranker
    result1 = (
        table.search(
            "Our father who art in heaven",
            query_type="hybrid",
            vector_column_name="vector",
        )
        .rerank(normalize="score")
        .to_pydantic(schema)
    )
    result2 = (  # noqa
        table.search(
            "Our father who art in heaven.",
            query_type="hybrid",
            vector_column_name="vector",
        )
        .rerank(normalize="rank")
        .to_pydantic(schema)
    )
    result3 = table.search(
        "Our father who art in heaven..",
        query_type="hybrid",
        vector_column_name="vector",
    ).to_pydantic(schema)

    assert result1 == result3  # 2 & 3 should be the same as they use score as score

    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search(query_type="hybrid", vector_column_name="vector")
        .vector(query_vector)
        .text(query)
        .limit(30)
        .rerank(reranker, normalize="score")
        .to_arrow()
    )
    assert len(result) == 30

    # Fail if both query and (vector or text) are provided
    with pytest.raises(ValueError):
        table.search(query, query_type="hybrid", vector_column_name="vector").vector(
            query_vector
        ).to_arrow()

    with pytest.raises(ValueError):
        table.search(query, query_type="hybrid", vector_column_name="vector").text(
            query
        ).to_arrow()
    ascending_relevance_err = (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )

    # Test with empty FTS results
    query = "abcxyz" * 100
    result = (
        table.search(query_type="hybrid", vector_column_name="vector")
        .vector(query_vector)
        .text(query)
        .limit(30)
        .rerank(reranker=reranker)
        .to_arrow()
    )
    # should return _relevance_score column
    assert "_relevance_score" in result.column_names
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        ascending_relevance_err
    )


def test_linear_combination(tmp_path):
    reranker = LinearCombinationReranker()

    vector_results = pa.Table.from_pydict(
        {
            "_rowid": [0, 1, 2, 3, 4],
            "_distance": [0.1, 0.2, 0.3, 0.4, 0.5],
            "_text": ["a", "b", "c", "d", "e"],
        }
    )

    fts_results = pa.Table.from_pydict(
        {
            "_rowid": [1, 2, 3, 4, 5],
            "_score": [0.1, 0.2, 0.3, 0.4, 0.5],
            "_text": ["b", "c", "d", "e", "f"],
        }
    )

    combined_results = reranker.merge_results(vector_results, fts_results, 1.0)
    assert len(combined_results) == 6
    assert "_rowid" in combined_results.column_names
    assert "_text" in combined_results.column_names
    assert "_distance" not in combined_results.column_names
    assert "_score" not in combined_results.column_names
    assert "_relevance_score" in combined_results.column_names

    _run_test_hybrid_reranker(reranker, tmp_path)


def test_rrf_reranker(tmp_path):
    reranker = RRFReranker()
    _run_test_hybrid_reranker(reranker, tmp_path)


def test_mrr_reranker(tmp_path):
    reranker = MRRReranker()
    _run_test_hybrid_reranker(reranker, tmp_path)

    # Test multi-vector part
    table, schema = get_test_table(tmp_path)
    query = "single player experience"
    rs1 = table.search(query, vector_column_name="vector").limit(10).with_row_id(True)
    rs2 = (
        table.search(query, vector_column_name="meta_vector")
        .limit(10)
        .with_row_id(True)
    )
    result = reranker.rerank_multivector([rs1, rs2])
    assert "_relevance_score" in result.column_names
    assert len(result) <= 20

    if len(result) > 1:
        assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
            "The _relevance_score should be descending."
        )

    # Test with duplicate results
    result_deduped = reranker.rerank_multivector([rs1, rs2, rs1])
    assert len(result_deduped) == len(result)


def test_mrr_reranker_empty_input():
    reranker = MRRReranker()
    with pytest.raises(ValueError, match="must not be empty"):
        reranker.rerank_multivector([])


def test_mrr_multivector_rewards_consensus():
    # Reciprocal ranks must be averaged across *all* ranking systems, treating a
    # missing system as 0. A document ranked first by every system must outrank a
    # document ranked first by only one of them.
    reranker = MRRReranker()

    def ranking(row_ids):
        return pa.table({"_rowid": pa.array(row_ids, type=pa.int64())})

    # Doc 1 is rank 1 in only the first system; doc 2 is rank 1 in two systems
    # and rank 2 in the third (strong cross-system consensus).
    rs1 = ranking([1, 2, 3])
    rs2 = ranking([2, 3, 4])
    rs3 = ranking([2, 5, 6])

    result = reranker.rerank_multivector([rs1, rs2, rs3])
    scores = {
        row_id: score
        for row_id, score in zip(
            result["_rowid"].to_pylist(),
            result["_relevance_score"].to_pylist(),
        )
    }

    # sum of reciprocal ranks / number of systems
    assert scores[1] == pytest.approx(1.0 / 3)
    assert scores[2] == pytest.approx((0.5 + 1.0 + 1.0) / 3)
    assert scores[2] > scores[1]
    # The consensus document ranks first overall.
    assert result["_rowid"].to_pylist()[0] == 2


def test_rrf_reranker_distance():
    data = pa.table(
        {
            "vector": pa.FixedSizeListArray.from_arrays(
                pc.random(32 * 1024).cast(pa.float32()), 32
            ),
            "text": pa.array(["hello"] * 1024),
        }
    )
    db = lancedb.connect("memory://")
    table = db.create_table("test", data)

    table.create_index(num_partitions=1, num_sub_vectors=2)
    table.create_fts_index("text")

    reranker = RRFReranker(return_score="all")

    hybrid_results = (
        table.search(query_type="hybrid")
        .vector([0.0] * 32)
        .text("hello")
        .with_row_id(True)
        .rerank(reranker)
        .to_list()
    )
    hybrid_distances = {row["_rowid"]: row["_distance"] for row in hybrid_results}
    hybrid_scores = {row["_rowid"]: row["_score"] for row in hybrid_results}

    vector_results = table.search([0.0] * 32).with_row_id(True).to_list()
    vector_distances = {row["_rowid"]: row["_distance"] for row in vector_results}

    fts_results = table.search("hello", query_type="fts").with_row_id(True).to_list()
    fts_scores = {row["_rowid"]: row["_score"] for row in fts_results}

    found_match = False
    for rowid, distance in hybrid_distances.items():
        if rowid in vector_distances:
            found_match = True
            assert distance == vector_distances[rowid], "Distance mismatch"
    assert found_match, "No results matched between hybrid and vector search"

    found_match = False
    for rowid, score in hybrid_scores.items():
        if rowid in fts_scores and fts_scores[rowid] is not None:
            found_match = True
            assert score == fts_scores[rowid], "Score mismatch"
    assert found_match, "No results matched between hybrid and fts search"

    # Test for empty fts results
    fts_results = (
        table.search("abcxyz" * 100, query_type="fts").with_row_id(True).to_list()
    )
    hybrid_results = (
        table.search(query_type="hybrid")
        .vector([0.0] * 32)
        .text("abcxyz" * 100)
        .with_row_id(True)
        .rerank(reranker)
        .to_list()
    )
    assert len(fts_results) == 0
    # confirm if _rowid, _score, _distance & _relevance_score are present in hybrid
    assert len(hybrid_results) > 0
    assert "_rowid" in hybrid_results[0]
    assert "_score" in hybrid_results[0]
    assert "_distance" in hybrid_results[0]
    assert "_relevance_score" in hybrid_results[0]


@pytest.mark.skipif(
    os.environ.get("COHERE_API_KEY") is None, reason="COHERE_API_KEY not set"
)
def test_cohere_reranker(tmp_path):
    pytest.importorskip("cohere")
    reranker = CohereReranker()
    table, schema = get_test_table(tmp_path)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


def test_cross_encoder_reranker(tmp_path):
    pytest.importorskip("sentence_transformers")
    reranker = CrossEncoderReranker()
    table, schema = get_test_table(tmp_path)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


def test_colbert_reranker(tmp_path):
    pytest.importorskip("rerankers")
    reranker = ColbertReranker()
    table, schema = get_test_table(tmp_path)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


def test_answerdotai_reranker(tmp_path):
    pytest.importorskip("rerankers")
    reranker = AnswerdotaiRerankers()
    table, schema = get_test_table(tmp_path)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None
    or os.environ.get("OPENAI_BASE_URL") is not None,
    reason="OPENAI_API_KEY not set",
)
def test_openai_reranker(tmp_path):
    pytest.importorskip("openai")
    table, schema = get_test_table(tmp_path)
    reranker = OpenaiReranker()
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("JINA_API_KEY") is None, reason="JINA_API_KEY not set"
)
def test_jina_reranker(tmp_path):
    pytest.importorskip("jina")
    table, schema = get_test_table(tmp_path)
    reranker = JinaReranker()
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("VOYAGE_API_KEY") is None, reason="VOYAGE_API_KEY not set"
)
def test_voyageai_reranker(tmp_path):
    pytest.importorskip("voyageai")
    reranker = VoyageAIReranker(model_name="rerank-2.5")
    table, schema = get_test_table(tmp_path)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


def test_empty_result_reranker():
    pytest.importorskip("sentence_transformers")
    db = lancedb.connect("memory://")

    # Define schema
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("text", pa.string()),
            ("vector", pa.list_(pa.float32(), 128)),  # 128-dimensional vector
        ]
    )

    # Create empty table with schema
    empty_table = db.create_table("empty_table", schema=schema, mode="overwrite")
    empty_table.create_fts_index("text", replace=True)
    for reranker in [
        CrossEncoderReranker(),
        # ColbertReranker(),
        # AnswerdotaiRerankers(),
        # OpenaiReranker(),
        # JinaReranker(),
        # VoyageAIReranker(model_name="rerank-2"),
    ]:
        results = (
            empty_table.search(list(range(128)))
            .limit(3)
            .rerank(reranker, "query")
            .to_arrow()
        )
        # check if empty set contains _relevance_score column
        assert "_relevance_score" in results.column_names
        assert len(results) == 0

        results = (
            empty_table.search("query", query_type="fts")
            .limit(3)
            .rerank(reranker)
            .to_arrow()
        )


def test_empty_hybrid_result_reranker():
    """Test that hybrid search with empty results after filtering doesn't crash.

    Regression test for https://github.com/lancedb/lancedb/issues/2425
    """
    from lancedb.query import LanceHybridQueryBuilder

    # Simulate empty vector and FTS results with the expected schema
    vector_schema = pa.schema(
        [
            ("text", pa.string()),
            ("vector", pa.list_(pa.float32(), 4)),
            ("_rowid", pa.uint64()),
            ("_distance", pa.float32()),
        ]
    )
    fts_schema = pa.schema(
        [
            ("text", pa.string()),
            ("vector", pa.list_(pa.float32(), 4)),
            ("_rowid", pa.uint64()),
            ("_score", pa.float32()),
        ]
    )
    empty_vector = pa.table(
        {
            "text": pa.array([], type=pa.string()),
            "vector": pa.array([], type=pa.list_(pa.float32(), 4)),
            "_rowid": pa.array([], type=pa.uint64()),
            "_distance": pa.array([], type=pa.float32()),
        },
        schema=vector_schema,
    )
    empty_fts = pa.table(
        {
            "text": pa.array([], type=pa.string()),
            "vector": pa.array([], type=pa.list_(pa.float32(), 4)),
            "_rowid": pa.array([], type=pa.uint64()),
            "_score": pa.array([], type=pa.float32()),
        },
        schema=fts_schema,
    )

    for reranker in [LinearCombinationReranker(), RRFReranker()]:
        result = LanceHybridQueryBuilder._combine_hybrid_results(
            fts_results=empty_fts,
            vector_results=empty_vector,
            norm="score",
            fts_query="nonexistent query",
            reranker=reranker,
            limit=10,
            with_row_ids=False,
        )
        assert len(result) == 0
        assert "_relevance_score" in result.column_names
        assert "_rowid" not in result.column_names

    # Also test with with_row_ids=True
    result = LanceHybridQueryBuilder._combine_hybrid_results(
        fts_results=empty_fts,
        vector_results=empty_vector,
        norm="score",
        fts_query="nonexistent query",
        reranker=LinearCombinationReranker(),
        limit=10,
        with_row_ids=True,
    )
    assert len(result) == 0
    assert "_relevance_score" in result.column_names
    assert "_rowid" in result.column_names


def test_cross_encoder_reranker_return_all(tmp_path):
    pytest.importorskip("sentence_transformers")
    reranker = CrossEncoderReranker(return_score="all")
    table, schema = get_test_table(tmp_path)
    query = "single player experience"
    result = (
        table.search(query, query_type="hybrid", vector_column_name="vector")
        .rerank(reranker=reranker)
        .to_arrow()
    )
    assert "_relevance_score" in result.column_names
    assert "_score" in result.column_names
    assert "_distance" in result.column_names


# ---------------------------------------------------------------------------
# Regression tests for LinearCombinationReranker scoring bugs (issue #3154)
# ---------------------------------------------------------------------------


def test_linear_combination_best_match_ranks_first():
    """
    The document that is BOTH the closest vector match AND the only FTS match
    must rank first.  Previously _combine_score subtracted from 1, inverting
    the ranking so the worst document ranked highest.
    """
    reranker = LinearCombinationReranker(weight=0.7, return_score="all")

    # rowid 0: perfect vector match, sole FTS match  → should rank 1st
    # rowid 1: mediocre vector, no FTS match
    # rowid 2: bad vector, no FTS match
    vector_results = pa.Table.from_pydict(
        {
            "_rowid": [0, 1, 2],
            "_distance": [0.0, 0.5, 0.9],
        }
    )
    fts_results = pa.Table.from_pydict(
        {
            "_rowid": [0],
            "_score": [1.0],
        }
    )

    combined = reranker.merge_results(vector_results, fts_results, fill=1.0)
    scores = dict(
        zip(
            combined["_rowid"].to_pylist(),
            combined["_relevance_score"].to_pylist(),
        )
    )

    # rowid 0 must have the highest relevance score
    assert scores[0] > scores[1], (
        f"Best match (rowid 0, score={scores[0]:.4f}) should beat "
        f"mid match (rowid 1, score={scores[1]:.4f})"
    )
    assert scores[1] > scores[2], (
        f"Mid match (rowid 1, score={scores[1]:.4f}) should beat "
        f"bad match (rowid 2, score={scores[2]:.4f})"
    )


def test_linear_combination_missing_fts_is_penalised():
    """
    A document with no FTS match must score *lower* than a document that
    has a mediocre FTS match, everything else being equal.  Previously
    missing-FTS entries used fill=1.0 directly, which gave them a reward
    (via the 1-(...) inversion) instead of a penalty.
    """
    reranker = LinearCombinationReranker(weight=0.5, return_score="all")

    vector_results = pa.Table.from_pydict(
        {
            "_rowid": [0, 1],
            "_distance": [0.2, 0.2],  # identical vector scores
        }
    )
    fts_results = pa.Table.from_pydict(
        {
            "_rowid": [0],  # rowid 1 has no FTS match
            "_score": [0.3],  # small FTS score
        }
    )

    combined = reranker.merge_results(vector_results, fts_results, fill=1.0)
    scores = dict(
        zip(
            combined["_rowid"].to_pylist(),
            combined["_relevance_score"].to_pylist(),
        )
    )

    # rowid 0 has a small FTS score; rowid 1 has none.
    # Even a small FTS contribution should beat having none at all.
    assert scores[0] > scores[1], (
        f"Document with FTS score (rowid 0, {scores[0]:.4f}) should beat "
        f"document with no FTS match (rowid 1, {scores[1]:.4f})"
    )
