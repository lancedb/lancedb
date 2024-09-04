import os
import random

import lancedb
import numpy as np
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
)
from lancedb.table import LanceTable

# Tests rely on FTS index
pytest.importorskip("lancedb.fts")


def get_test_table(tmp_path, use_tantivy):
    db = lancedb.connect(tmp_path)
    # Create a LanceDB table schema with a vector and a text column
    emb = EmbeddingFunctionRegistry.get_instance().get("test")()
    meta_emb = EmbeddingFunctionRegistry.get_instance().get("test")()

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
    err = (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), err

    # Vector search setting
    result = (
        table.search(query, vector_column_name="vector")
        .rerank(reranker=reranker)
        .limit(30)
        .to_arrow()
    )
    assert len(result) == 30
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), err
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
    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), err

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
    assert len(result_deduped) < 20
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
        .rerank(normalize="score")
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

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_linear_combination(tmp_path, use_tantivy):
    reranker = LinearCombinationReranker()
    _run_test_hybrid_reranker(reranker, tmp_path, use_tantivy)


@pytest.mark.parametrize("use_tantivy", [True, False])
def test_rrf_reranker(tmp_path, use_tantivy):
    reranker = RRFReranker()
    _run_test_hybrid_reranker(reranker, tmp_path, use_tantivy)


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
    os.environ.get("OPENAI_API_KEY") is None, reason="OPENAI_API_KEY not set"
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
