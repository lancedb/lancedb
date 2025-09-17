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
)
from lancedb.table import LanceTable

# Tests rely on FTS index
pytest.importorskip("lancedb.fts")


def get_test_table(tmp_path, use_tantivy):
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
    table.create_fts_index("text", use_tantivy=use_tantivy)

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


def _run_test_hybrid_reranker(reranker, tmp_path, use_tantivy):
    table, schema = get_test_table(tmp_path, use_tantivy)
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


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_linear_combination(tmp_path, use_tantivy):
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

    _run_test_hybrid_reranker(reranker, tmp_path, use_tantivy)


def test_linear_combination_score():
    reranker = LinearCombinationReranker(weight=0.7, return_score="all")

    vector_results = pa.Table.from_pydict(
        {
            "_rowid": [1, 2, 3],
            "_distance": [0.2, 0.8, 0.5],
        }
    )
    fts_results = pa.Table.from_pydict(
        {
            "_rowid": [1, 3, 4],
            "_score": [0.6, 0.9, 0.1],
        }
    )

    combined = reranker.merge_results(vector_results, fts_results, fill=1.0)

    # expect relevance per row (sorted desc):
    # 1: 0.7*0.8 + 0.3*0.6 = 0.74
    # 3: 0.7*0.5 + 0.3*0.9 = 0.62
    # 2: 0.7*0.2 + 0.3*0.0 = 0.14 (fts missing -> 0.0)
    # 4: 0.7*0.0 + 0.3*0.1 = 0.03 (vector missing -> invert(fill=1.0)=0.0)
    rowids = combined.column("_rowid").to_pylist()
    scores = combined.column("_relevance_score").to_pylist()

    assert rowids == [1, 3, 2, 4]
    assert pytest.approx(scores[0], rel=1e-6) == 0.74
    assert pytest.approx(scores[1], rel=1e-6) == 0.62
    assert pytest.approx(scores[2], rel=1e-6) == 0.14
    assert pytest.approx(scores[3], rel=1e-6) == 0.03


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_rrf_reranker(tmp_path, use_tantivy):
    reranker = RRFReranker()
    _run_test_hybrid_reranker(reranker, tmp_path, use_tantivy)


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
    table.create_fts_index("text", use_tantivy=False)

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
@pytest.mark.parametrize("use_tantivy", [True, False])
def test_cohere_reranker(tmp_path, use_tantivy):
    pytest.importorskip("cohere")
    reranker = CohereReranker()
    table, schema = get_test_table(tmp_path, use_tantivy)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_cross_encoder_reranker(tmp_path, use_tantivy):
    pytest.importorskip("sentence_transformers")
    reranker = CrossEncoderReranker()
    table, schema = get_test_table(tmp_path, use_tantivy)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_colbert_reranker(tmp_path, use_tantivy):
    pytest.importorskip("rerankers")
    reranker = ColbertReranker()
    table, schema = get_test_table(tmp_path, use_tantivy)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_answerdotai_reranker(tmp_path, use_tantivy):
    pytest.importorskip("rerankers")
    reranker = AnswerdotaiRerankers()
    table, schema = get_test_table(tmp_path, use_tantivy)
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None
    or os.environ.get("OPENAI_BASE_URL") is not None,
    reason="OPENAI_API_KEY not set",
)
@pytest.mark.parametrize("use_tantivy", [True, False])
def test_openai_reranker(tmp_path, use_tantivy):
    pytest.importorskip("openai")
    table, schema = get_test_table(tmp_path, use_tantivy)
    reranker = OpenaiReranker()
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("JINA_API_KEY") is None, reason="JINA_API_KEY not set"
)
@pytest.mark.parametrize("use_tantivy", [True, False])
def test_jina_reranker(tmp_path, use_tantivy):
    pytest.importorskip("jina")
    table, schema = get_test_table(tmp_path, use_tantivy)
    reranker = JinaReranker()
    _run_test_reranker(reranker, table, "single player experience", None, schema)


@pytest.mark.skipif(
    os.environ.get("VOYAGE_API_KEY") is None, reason="VOYAGE_API_KEY not set"
)
@pytest.mark.parametrize("use_tantivy", [True, False])
def test_voyageai_reranker(tmp_path, use_tantivy):
    pytest.importorskip("voyageai")
    reranker = VoyageAIReranker(model_name="rerank-2")
    table, schema = get_test_table(tmp_path, use_tantivy)
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
    empty_table.create_fts_index("text", use_tantivy=False, replace=True)
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


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_cross_encoder_reranker_return_all(tmp_path, use_tantivy):
    pytest.importorskip("sentence_transformers")
    reranker = CrossEncoderReranker(return_score="all")
    table, schema = get_test_table(tmp_path, use_tantivy)
    query = "single player experience"
    result = (
        table.search(query, query_type="hybrid", vector_column_name="vector")
        .rerank(reranker=reranker)
        .to_arrow()
    )
    assert "_relevance_score" in result.column_names
    assert "_score" in result.column_names
    assert "_distance" in result.column_names
