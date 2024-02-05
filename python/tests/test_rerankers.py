import os

import numpy as np
import pytest

import lancedb
from lancedb.conftest import MockTextEmbeddingFunction  # noqa
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import (
    CohereReranker,
    ColbertReranker,
    CrossEncoderReranker,
    OpenaiReranker,
)
from lancedb.table import LanceTable


def get_test_table(tmp_path):
    db = lancedb.connect(tmp_path)
    # Create a LanceDB table schema with a vector and a text column
    emb = EmbeddingFunctionRegistry.get_instance().get("test")()

    class MyTable(LanceModel):
        text: str = emb.SourceField()
        vector: Vector(emb.ndims()) = emb.VectorField()

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
    table.add([{"text": p} for p in phrases])

    # Create a fts index
    table.create_fts_index("text")

    return table, MyTable


def test_linear_combination(tmp_path):
    table, schema = get_test_table(tmp_path)
    # The default reranker
    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score")
        .to_pydantic(schema)
    )
    result2 = (  # noqa
        table.search("Our father who art in heaven.", query_type="hybrid")
        .rerank(normalize="rank")
        .to_pydantic(schema)
    )
    result3 = table.search(
        "Our father who art in heaven..", query_type="hybrid"
    ).to_pydantic(schema)

    assert result1 == result3  # 2 & 3 should be the same as they use score as score

    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search((query_vector, query))
        .limit(30)
        .rerank(normalize="score")
        .to_arrow()
    )

    assert len(result) == 30

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )


@pytest.mark.skipif(
    os.environ.get("COHERE_API_KEY") is None, reason="COHERE_API_KEY not set"
)
def test_cohere_reranker(tmp_path):
    pytest.importorskip("cohere")
    table, schema = get_test_table(tmp_path)
    # The default reranker
    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score", reranker=CohereReranker())
        .to_pydantic(schema)
    )
    result2 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(reranker=CohereReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2

    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search((query_vector, query))
        .limit(30)
        .rerank(reranker=CohereReranker())
        .to_arrow()
    )

    assert len(result) == 30

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )


def test_cross_encoder_reranker(tmp_path):
    pytest.importorskip("sentence_transformers")
    table, schema = get_test_table(tmp_path)
    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score", reranker=CrossEncoderReranker())
        .to_pydantic(schema)
    )
    result2 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(reranker=CrossEncoderReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2

    # test explicit hybrid query
    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search((query_vector, query), query_type="hybrid")
        .limit(30)
        .rerank(reranker=CrossEncoderReranker())
        .to_arrow()
    )

    assert len(result) == 30

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )


def test_colbert_reranker(tmp_path):
    pytest.importorskip("transformers")
    table, schema = get_test_table(tmp_path)
    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score", reranker=ColbertReranker())
        .to_pydantic(schema)
    )
    result2 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(reranker=ColbertReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2

    # test explicit hybrid query
    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search((query_vector, query))
        .limit(30)
        .rerank(reranker=ColbertReranker())
        .to_arrow()
    )

    assert len(result) == 30

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )


@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None, reason="OPENAI_API_KEY not set"
)
def test_openai_reranker(tmp_path):
    pytest.importorskip("openai")
    table, schema = get_test_table(tmp_path)
    result1 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="score", reranker=OpenaiReranker())
        .to_pydantic(schema)
    )
    result2 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(reranker=OpenaiReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2

    # test explicit hybrid query
    query = "Our father who art in heaven"
    query_vector = table.to_pandas()["vector"][0]
    result = (
        table.search((query_vector, query))
        .limit(30)
        .rerank(reranker=OpenaiReranker())
        .to_arrow()
    )

    assert len(result) == 30

    assert np.all(np.diff(result.column("_relevance_score").to_numpy()) <= 0), (
        "The _relevance_score column of the results returned by the reranker "
        "represents the relevance of the result to the query & should "
        "be descending."
    )
