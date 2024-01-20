import os

import pytest

import lancedb
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import CohereReranker, CrossEncoderReranker
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

    # Create a list of 10 unique english phrases
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
    result2 = (
        table.search("Our father who art in heaven", query_type="hybrid")
        .rerank(normalize="rank")
        .to_pydantic(schema)
    )
    result3 = table.search(
        "Our father who art in heaven", query_type="hybrid"
    ).to_pydantic(schema)
    assert result1 == result2 == result3


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
        .rerank(normalize="rank", reranker=CohereReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2


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
        .rerank(normalize="rank", reranker=CrossEncoderReranker())
        .to_pydantic(schema)
    )
    assert result1 == result2
