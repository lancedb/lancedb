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
    tbl = db.create_table("vector_search", data=data, mode="overwrite")
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
    tbl = db.create_table("documents", data=data, mode="overwrite")
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
    async_tbl = await async_db.create_table(
        "vector_search_async", data=data, mode="overwrite"
    )
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

    async_tbl = await async_db.create_table(
        "documents_async", data=data, mode="overwrite"
    )
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
    table.create_fts_index("text", replace=True)

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
    table.create_fts_index("desc", replace=True)

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
        mode="overwrite",
    )

    table.create_fts_index("text")
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
    table.create_fts_index("text", with_position=True, replace=True)
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
        mode="overwrite",
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
    )
    # --8<-- [end:fts_config_stem_async]
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


def _vectors(n, dim, seed=0, column="vector"):
    rng = np.random.default_rng(seed)
    return [
        {column: rng.random(dim).astype("float32"), "id": i} for i in range(n)
    ]


def _build_indexed_table(db, name, dim=128, n=512):
    tbl = db.create_table(name, _vectors(n, dim), mode="overwrite")
    tbl.create_index(metric="cosine", num_partitions=4, num_sub_vectors=8)
    return tbl


def test_vs_distance_metric_and_brute_force(tmp_db):
    tbl = tmp_db.create_table("vs_metric", _vectors(64, 1536), mode="overwrite")

    # --8<-- [start:configure_distance_metric]
    tbl.search(np.random.random((1536))).distance_type("cosine").limit(10).to_list()
    # --8<-- [end:configure_distance_metric]

    # --8<-- [start:brute_force_search]
    tbl.search(np.random.random((1536))).limit(3).to_list()
    # --8<-- [end:brute_force_search]


def test_vs_select_vector_column(tmp_db):
    db = tmp_db

    # --8<-- [start:select_vector_column]
    import pyarrow as pa

    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field(
            "image",
            pa.struct([pa.field("embedding", pa.list_(pa.float32(), 2))]),
        ),
    ])
    table = db.create_table(
        "nested",
        data=[{"id": 0, "image": {"embedding": [0.0, 1.0]}}],
        schema=schema,
    )

    # Inferred: the only vector leaf is `image.embedding`.
    table.search([0.0, 1.0]).limit(1).to_list()

    # Explicit: required when more than one vector column matches.
    table.search([0.0, 1.0], vector_column_name="image.embedding").limit(1).to_list()
    # --8<-- [end:select_vector_column]


def test_vs_index_nested_column(tmp_db):
    dim = 16
    schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "image",
                pa.struct([pa.field("embedding", pa.list_(pa.float32(), dim))]),
            ),
        ]
    )
    rng = np.random.default_rng(0)
    data = [
        {"id": i, "image": {"embedding": rng.random(dim).astype("float32").tolist()}}
        for i in range(512)
    ]
    table = tmp_db.create_table(
        "nested_index", data=data, schema=schema, mode="overwrite"
    )

    # --8<-- [start:index_nested_column]
    table.create_index(vector_column_name="image.embedding")
    # --8<-- [end:index_nested_column]

    assert table.list_indices()


def test_vs_indexed_queries(tmp_db):
    table = _build_indexed_table(tmp_db, "vs_indexed", dim=128, n=512)
    embedding = np.random.random(128)

    # --8<-- [start:exact_vs_approximate_distances]
    # Indexed ANN search without refinement (fast, approximate `_distance`)
    fast_results = (
        table.search(embedding)
        .limit(10)
        .to_pandas()
    )

    # Recompute distances on full vectors for reranked candidates
    exact_distance_results = (
        table.search(embedding)
        .limit(10)
        .refine_factor(1)
        .to_pandas()
    )

    # Rerank a larger candidate set for better recall (higher latency)
    higher_recall_results = (
        table.search(embedding)
        .limit(10)
        .refine_factor(20)
        .to_pandas()
    )
    # --8<-- [end:exact_vs_approximate_distances]

    # --8<-- [start:bypass_vector_index]
    table.search(embedding).bypass_vector_index().limit(5).to_pandas()
    # --8<-- [end:bypass_vector_index]

    assert len(fast_results) == 10
    assert len(exact_distance_results) == 10
    assert len(higher_recall_results) == 10


def test_vs_fast_search(tmp_db, monkeypatch):
    table = _build_indexed_table(tmp_db, "vs_fast", dim=128, n=512)
    embedding = np.random.random(128)

    # `fast_search` is an Enterprise/async query flag; strip it so the snippet
    # runs against a local OSS table without changing what readers see.
    real_search = table.search
    monkeypatch.setattr(
        table,
        "search",
        lambda *a, **k: real_search(
            *a, **{key: val for key, val in k.items() if key != "fast_search"}
        ),
    )

    # --8<-- [start:fast_search]
    table.search(embedding, fast_search=True).limit(5).to_pandas()
    # --8<-- [end:fast_search]


def test_vs_distance_range(tmp_db):
    tbl = tmp_db.create_table("vs_distance_range", _vectors(256, 256), mode="overwrite")

    # --8<-- [start:search_distance_range]
    query = np.random.random(256)

    # Search for the vectors within the range of [0.1, 0.5)
    tbl.search(query).distance_range(0.1, 0.5).to_arrow()

    # Search for the vectors with the distance less than 0.5
    tbl.search(query).distance_range(upper_bound=0.5).to_arrow()

    # Search for the vectors with the distance greater or equal to 0.1
    tbl.search(query).distance_range(lower_bound=0.1).to_arrow()
    # --8<-- [end:search_distance_range]


def test_vs_multivector_search(tmp_db):
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", pa.list_(pa.list_(pa.float32(), 256))),
        ]
    )
    rng = np.random.default_rng(0)
    data = [{"id": i, "vector": rng.random(size=(2, 256)).tolist()} for i in range(64)]
    tbl = tmp_db.create_table(
        "vs_multivector", data=data, schema=schema, mode="overwrite"
    )

    # --8<-- [start:multivector_search]
    query_multi = np.random.random(size=(2, 256))
    results_multi = tbl.search(query_multi).limit(5).to_pandas()
    # --8<-- [end:multivector_search]

    assert len(results_multi) <= 5


def test_vs_binary_search(tmp_db):
    db = tmp_db

    # --8<-- [start:search_binary_vectors]
    import numpy as np
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            # for dim=256, lance stores every 8 bits in a byte
            # so the vector field should be a list of 256 / 8 = 32 bytes
            pa.field("vector", pa.list_(pa.uint8(), 32)),
        ]
    )
    tbl = db.create_table("my_binary_vectors", schema=schema)

    data = []
    for i in range(1024):
        vector = np.random.randint(0, 2, size=256)
        # pack the binary vector into bytes to save space
        packed_vector = np.packbits(vector)
        data.append(
            {
                "id": i,
                "vector": packed_vector,
            }
        )
    tbl.add(data)

    query = np.random.randint(0, 2, size=256)
    packed_query = np.packbits(query)
    tbl.search(packed_query).distance_type("hamming").to_arrow()
    # --8<-- [end:search_binary_vectors]


def test_vs_enterprise_filtering(tmp_db, monkeypatch):
    import sys
    import types

    dim = 384
    rng = np.random.default_rng(0)
    rows = [
        {
            "vector": rng.random(dim).astype("float32"),
            "text": f"story {i}",
            "keywords": f"kw{i}",
            "label": i % 4,
        }
        for i in range(50)
    ]
    tmp_db.create_table("lancedb-enterprise-quickstart", data=rows, mode="overwrite")
    db = tmp_db

    # Mock the Hugging Face dataset loader to avoid network downloads.
    class _FakeDataset:
        def __init__(self, n):
            self._emb = [rng.random(dim).astype("float32") for _ in range(n)]
            self._kw = [f"kw{i}" for i in range(n)]

        def __getitem__(self, key):
            if isinstance(key, str):
                return {"keywords_embeddings": self._emb, "keywords": self._kw}[key]
            return {"keywords": self._kw[key], "keywords_embeddings": self._emb[key]}

    fake_datasets = types.ModuleType("datasets")
    fake_datasets.load_dataset = lambda *a, **k: _FakeDataset(10)
    monkeypatch.setitem(sys.modules, "datasets", fake_datasets)

    # --8<-- [start:vector_search_prefilter]
    from datasets import load_dataset

    # Load query vector from dataset
    query_dataset = load_dataset("sunhaozhepy/ag_news_sbert_keywords_embeddings", split="test[5000:5001]")
    print(f"Query keywords: {query_dataset[0]['keywords']}")
    query_embed = query_dataset["keywords_embeddings"][0]

    # Open table and perform search
    table_name = "lancedb-enterprise-quickstart"
    table = db.open_table(table_name)

    # Vector search with filters (pre-filtering is the default)
    search_results = (
        table.search(query_embed)
        .where("label > 2")
        .select(["text", "keywords", "label"])
        .limit(5)
        .to_pandas()
    )

    print("Search results (with pre-filtering):")
    print(search_results)
    # --8<-- [end:vector_search_prefilter]

    # --8<-- [start:vector_search_postfilter]
    results_post_filtered = (
        table.search(query_embed)
        .where("label > 1", prefilter=False)
        .select(["text", "keywords", "label"])
        .limit(5)
        .to_pandas()
    )

    print("Vector search results with post-filter:")
    print(results_post_filtered)
    # --8<-- [end:vector_search_postfilter]

    # --8<-- [start:batch_search]
    # Load a batch of query embeddings
    query_dataset = load_dataset(
        "sunhaozhepy/ag_news_sbert_keywords_embeddings", split="test[5000:5005]"
    )
    query_embeds = query_dataset["keywords_embeddings"]
    batch_results = table.search(query_embeds).limit(5).to_pandas()
    print(batch_results)
    # --8<-- [end:batch_search]


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
