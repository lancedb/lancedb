# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# --8<-- [start:import-lancedb]
import lancedb

# --8<-- [end:import-lancedb]
# --8<-- [start:import-numpy]
from lancedb.query import BoostQuery, MatchQuery
import numpy as np
import pyarrow as pa

# --8<-- [end:import-numpy]
# --8<-- [start:import-datetime]
from datetime import datetime

# --8<-- [end:import-datetime]
# --8<-- [start:import-lancedb-pydantic]
from lancedb.pydantic import Vector, LanceModel

# --8<-- [end:import-lancedb-pydantic]
# --8<-- [start:import-pydantic-base-model]
from pydantic import BaseModel

# --8<-- [end:import-pydantic-base-model]
# --8<-- [start:import-lancedb-fts]
from lancedb.index import FTS

# --8<-- [end:import-lancedb-fts]
# --8<-- [start:import-os]
import os

# --8<-- [end:import-os]
# --8<-- [start:import-embeddings]
from lancedb.embeddings import get_registry

# --8<-- [end:import-embeddings]
import pytest


# --8<-- [start:class-definition]
class Metadata(BaseModel):
    source: str
    timestamp: datetime


class Document(BaseModel):
    content: str
    meta: Metadata


class LanceSchema(LanceModel):
    id: str
    vector: Vector(1536)
    payload: Document


# --8<-- [end:class-definition]


def test_vector_search():
    # --8<-- [start:exhaustive_search]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    data = [
        {"vector": row, "item": f"item {i}"}
        for i, row in enumerate(np.random.random((10_000, 1536)).astype("float32"))
    ]
    tbl = db.create_table("vector_search", data=data)
    tbl.search(np.random.random((1536))).limit(10).to_list()
    # --8<-- [end:exhaustive_search]
    # --8<-- [start:exhaustive_search_cosine]
    tbl.search(np.random.random((1536))).distance_type("cosine").limit(10).to_list()
    # --8<-- [end:exhaustive_search_cosine]
    # --8<-- [start:create_table_with_nested_schema]
    # Let's add 100 sample rows to our dataset
    data = [
        LanceSchema(
            id=f"id{i}",
            vector=np.random.randn(1536),
            payload=Document(
                content=f"document{i}",
                meta=Metadata(source=f"source{i % 10}", timestamp=datetime.now()),
            ),
        )
        for i in range(100)
    ]

    # Synchronous client
    tbl = db.create_table("documents", data=data)
    # --8<-- [end:create_table_with_nested_schema]
    # --8<-- [start:search_result_as_pyarrow]
    tbl.search(np.random.randn(1536)).to_arrow()
    # --8<-- [end:search_result_as_pyarrow]
    # --8<-- [start:search_result_as_pandas]
    tbl.search(np.random.randn(1536)).to_pandas()
    # --8<-- [end:search_result_as_pandas]
    # --8<-- [start:search_result_as_pandas_flatten_true]
    tbl.search(np.random.randn(1536)).to_pandas(flatten=True)
    # --8<-- [end:search_result_as_pandas_flatten_true]
    # --8<-- [start:search_result_as_pandas_flatten_1]
    tbl.search(np.random.randn(1536)).to_pandas(flatten=1)
    # --8<-- [end:search_result_as_pandas_flatten_1]
    # --8<-- [start:search_result_as_list]
    tbl.search(np.random.randn(1536)).to_list()
    # --8<-- [end:search_result_as_list]
    # --8<-- [start:search_result_as_pydantic]
    tbl.search(np.random.randn(1536)).to_pydantic(LanceSchema)
    # --8<-- [end:search_result_as_pydantic]


@pytest.mark.asyncio
async def test_vector_search_async():
    # --8<-- [start:exhaustive_search_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    data = [
        {"vector": row, "item": f"item {i}"}
        for i, row in enumerate(np.random.random((10_000, 1536)).astype("float32"))
    ]
    async_tbl = await async_db.create_table("vector_search_async", data=data)
    (await (await async_tbl.search(np.random.random((1536)))).limit(10).to_list())
    # --8<-- [end:exhaustive_search_async]
    # --8<-- [start:exhaustive_search_async_cosine]
    (
        await (await async_tbl.search(np.random.random((1536))))
        .distance_type("cosine")
        .limit(10)
        .to_list()
    )
    # --8<-- [end:exhaustive_search_async_cosine]
    # --8<-- [start:create_table_async_with_nested_schema]
    # Let's add 100 sample rows to our dataset
    data = [
        LanceSchema(
            id=f"id{i}",
            vector=np.random.randn(1536),
            payload=Document(
                content=f"document{i}",
                meta=Metadata(source=f"source{i % 10}", timestamp=datetime.now()),
            ),
        )
        for i in range(100)
    ]

    async_tbl = await async_db.create_table("documents_async", data=data)
    # --8<-- [end:create_table_async_with_nested_schema]
    # --8<-- [start:search_result_async_as_pyarrow]
    await (await async_tbl.search(np.random.randn(1536))).to_arrow()
    # --8<-- [end:search_result_async_as_pyarrow]
    # --8<-- [start:search_result_async_as_pandas]
    await (await async_tbl.search(np.random.randn(1536))).to_pandas()
    # --8<-- [end:search_result_async_as_pandas]
    # --8<-- [start:search_result_async_as_list]
    await (await async_tbl.search(np.random.randn(1536))).to_list()
    # --8<-- [end:search_result_async_as_list]


def test_fts_fuzzy_query():
    uri = "data/fuzzy-example"
    db = lancedb.connect(uri)

    table = db.create_table(
        "my_table_fts_fuzzy",
        data=pa.table(
            {
                "text": [
                    "fa",
                    "fo",  # spellchecker:disable-line
                    "fob",
                    "focus",
                    "foo",
                    "food",
                    "foul",
                ]
            }
        ),
        mode="overwrite",
    )
    table.create_fts_index("text", use_tantivy=False, replace=True)

    results = table.search(MatchQuery("foo", "text", fuzziness=1)).to_pandas()
    assert len(results) == 4
    assert set(results["text"].to_list()) == {
        "foo",
        "fo",  # 1 deletion # spellchecker:disable-line
        "fob",  # 1 substitution
        "food",  # 1 insertion
    }


def test_fts_boost_query():
    uri = "data/boost-example"
    db = lancedb.connect(uri)

    table = db.create_table(
        "my_table_fts_boost",
        data=pa.table(
            {
                "title": [
                    "The Hidden Gems of Travel",
                    "Exploring Nature's Wonders",
                    "Cultural Treasures Unveiled",
                    "The Nightlife Chronicles",
                    "Scenic Escapes and Challenges",
                ],
                "desc": [
                    "A vibrant city with occasional traffic jams.",
                    "Beautiful landscapes but overpriced tourist spots.",
                    "Rich cultural heritage but humid summers.",
                    "Bustling nightlife but noisy streets.",
                    "Scenic views but limited public transport options.",
                ],
            }
        ),
        mode="overwrite",
    )
    table.create_fts_index("desc", use_tantivy=False, replace=True)

    results = table.search(
        BoostQuery(
            MatchQuery("beautiful, cultural, nightlife", "desc"),
            MatchQuery("bad traffic jams, overpriced", "desc"),
        ),
    ).to_pandas()

    # we will hit 3 results because the positive query has 3 hits
    assert len(results) == 3
    # the one containing "overpriced" will be negatively boosted,
    # so it will be the last one
    assert (
        results["desc"].to_list()[2]
        == "Beautiful landscapes but overpriced tourist spots."
    )


def test_fts_native():
    # --8<-- [start:basic_fts]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)

    table = db.create_table(
        "my_table_fts",
        data=[
            {"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"},
            {"vector": [5.9, 26.5], "text": "There are several kittens playing"},
        ],
    )

    # passing `use_tantivy=False` to use lance FTS index
    # `use_tantivy=True` by default
    table.create_fts_index("text", use_tantivy=False)
    table.search("puppy").limit(10).select(["text"]).to_list()
    # [{'text': 'Frodo was a happy puppy', '_score': 0.6931471824645996}]
    # ...
    # --8<-- [end:basic_fts]
    # --8<-- [start:fts_config_stem]
    table.create_fts_index("text", tokenizer_name="en_stem", replace=True)
    # --8<-- [end:fts_config_stem]
    # --8<-- [start:fts_config_folding]
    table.create_fts_index(
        "text",
        use_tantivy=False,
        language="French",
        stem=True,
        ascii_folding=True,
        replace=True,
    )
    # --8<-- [end:fts_config_folding]
    # --8<-- [start:fts_prefiltering]
    table.search("puppy").limit(10).where("text='foo'", prefilter=True).to_list()
    # --8<-- [end:fts_prefiltering]
    # --8<-- [start:fts_postfiltering]
    table.search("puppy").limit(10).where("text='foo'", prefilter=False).to_list()
    # --8<-- [end:fts_postfiltering]
    # --8<-- [start:fts_with_position]
    table.create_fts_index("text", use_tantivy=False, with_position=True, replace=True)
    # --8<-- [end:fts_with_position]
    # --8<-- [start:fts_incremental_index]
    table.add([{"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"}])
    table.optimize()
    # --8<-- [end:fts_incremental_index]


@pytest.mark.asyncio
async def test_fts_native_async():
    # --8<-- [start:basic_fts_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)

    async_tbl = await async_db.create_table(
        "my_table_fts_async",
        data=[
            {"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"},
            {"vector": [5.9, 26.5], "text": "There are several kittens playing"},
        ],
    )

    # async API uses our native FTS algorithm
    await async_tbl.create_index("text", config=FTS())
    await (await async_tbl.search("puppy")).select(["text"]).limit(10).to_list()
    # [{'text': 'Frodo was a happy puppy', '_score': 0.6931471824645996}]
    # ...
    # --8<-- [end:basic_fts_async]
    # --8<-- [start:fts_config_stem_async]
    await async_tbl.create_index(
        "text", config=FTS(language="English", stem=True, remove_stop_words=True)
    )  # --8<-- [end:fts_config_stem_async]
    # --8<-- [start:fts_config_folding_async]
    await async_tbl.create_index(
        "text", config=FTS(language="French", stem=True, ascii_folding=True)
    )
    # --8<-- [end:fts_config_folding_async]
    # --8<-- [start:fts_prefiltering_async]
    await (await async_tbl.search("puppy")).limit(10).where("text='foo'").to_list()
    # --8<-- [end:fts_prefiltering_async]
    # --8<-- [start:fts_postfiltering_async]
    await (
        (await async_tbl.search("puppy"))
        .limit(10)
        .where("text='foo'")
        .postfilter()
        .to_list()
    )
    # --8<-- [end:fts_postfiltering_async]
    # --8<-- [start:fts_with_position_async]
    await async_tbl.create_index("text", config=FTS(with_position=True))
    # --8<-- [end:fts_with_position_async]
    # --8<-- [start:fts_incremental_index_async]
    await async_tbl.add([{"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"}])
    await async_tbl.optimize()
    # --8<-- [end:fts_incremental_index_async]


@pytest.mark.skip()
def test_hybrid_search():
    # --8<-- [start:import-openai]
    import openai

    # --8<-- [end:import-openai]
    # --8<-- [start:openai-embeddings]
    # Ingest embedding function in LanceDB table
    # Configuring the environment variable OPENAI_API_KEY
    if "OPENAI_API_KEY" not in os.environ:
        # OR set the key here as a variable
        openai.api_key = "sk-..."
    embeddings = get_registry().get("openai").create()

    # --8<-- [end:openai-embeddings]
    # --8<-- [start:class-Documents]
    class Documents(LanceModel):
        vector: Vector(embeddings.ndims()) = embeddings.VectorField()
        text: str = embeddings.SourceField()

    # --8<-- [end:class-Documents]
    # --8<-- [start:basic_hybrid_search]
    data = [
        {"text": "rebel spaceships striking from a hidden base"},
        {"text": "have won their first victory against the evil Galactic Empire"},
        {"text": "during the battle rebel spies managed to steal secret plans"},
        {"text": "to the Empire's ultimate weapon the Death Star"},
    ]
    uri = "data/sample-lancedb"
    db = lancedb.connect(uri)
    table = db.create_table("documents", schema=Documents)
    # ingest docs with auto-vectorization
    table.add(data)
    # Create a fts index before the hybrid search
    table.create_fts_index("text")
    # hybrid search with default re-ranker
    table.search("flower moon", query_type="hybrid").to_pandas()
    # --8<-- [end:basic_hybrid_search]
    # --8<-- [start:hybrid_search_pass_vector_text]
    vector_query = [0.1, 0.2, 0.3, 0.4, 0.5]
    text_query = "flower moon"
    (
        table.search(query_type="hybrid")
        .vector(vector_query)
        .text(text_query)
        .limit(5)
        .to_pandas()
    )
    # --8<-- [end:hybrid_search_pass_vector_text]


@pytest.mark.skip
async def test_hybrid_search_async():
    import openai

    # --8<-- [start:openai-embeddings]
    # Ingest embedding function in LanceDB table
    # Configuring the environment variable OPENAI_API_KEY
    if "OPENAI_API_KEY" not in os.environ:
        # OR set the key here as a variable
        openai.api_key = "sk-..."
    embeddings = get_registry().get("openai").create()

    # --8<-- [end:openai-embeddings]
    # --8<-- [start:class-Documents]
    class Documents(LanceModel):
        vector: Vector(embeddings.ndims()) = embeddings.VectorField()
        text: str = embeddings.SourceField()

    # --8<-- [end:class-Documents]
    # --8<-- [start:basic_hybrid_search_async]
    uri = "data/sample-lancedb"
    async_db = await lancedb.connect_async(uri)
    data = [
        {"text": "rebel spaceships striking from a hidden base"},
        {"text": "have won their first victory against the evil Galactic Empire"},
        {"text": "during the battle rebel spies managed to steal secret plans"},
        {"text": "to the Empire's ultimate weapon the Death Star"},
    ]
    async_tbl = await async_db.create_table("documents_async", schema=Documents)
    # ingest docs with auto-vectorization
    await async_tbl.add(data)
    # Create a fts index before the hybrid search
    await async_tbl.create_index("text", config=FTS())
    text_query = "flower moon"
    # hybrid search with default re-ranker
    await (await async_tbl.search("flower moon", query_type="hybrid")).to_pandas()
    # --8<-- [end:basic_hybrid_search_async]
    # --8<-- [start:hybrid_search_pass_vector_text_async]
    vector_query = [0.1, 0.2, 0.3, 0.4, 0.5]
    text_query = "flower moon"
    await (
        async_tbl.query()
        .nearest_to(vector_query)
        .nearest_to_text(text_query)
        .limit(5)
        .to_pandas()
    )
    # --8<-- [end:hybrid_search_pass_vector_text_async]
