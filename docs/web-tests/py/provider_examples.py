# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Reader examples with explicit placeholders.

These functions are documentation source. test_provider_examples.py runs the
reranker examples offline, with stand-ins for the model and the provider, to
check that each one writes only to its own new directory. Corresponding opt-in
integration tests remain in test_integrations.py and retain real credential
loading. Replace placeholders before running these examples.
"""


def embedding_ibm_usage():
    # --8<-- [start:embedding_ibm_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    registry = EmbeddingFunctionRegistry.get_instance()
    registry.set_var("watsonx-api-key", "your-api-key")
    watsonx_embed = registry.get("watsonx").create(
        name="ibm/slate-125m-english-rtrvr",
        api_key="$var:watsonx-api-key",
        project_id="your-project-id",
    )

    class TextModel(LanceModel):
        text: str = watsonx_embed.SourceField()
        vector: Vector(watsonx_embed.ndims()) = watsonx_embed.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "watsonx-demo"))
    tbl = db.create_table("watsonx_test", schema=TextModel, mode="overwrite")
    tbl.add(data)

    rs = tbl.search("hello").limit(1).to_pandas()
    print(rs.head())
    # --8<-- [end:embedding_ibm_usage]


def embedding_jina_text():
    # --8<-- [start:embedding_jina_text]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import EmbeddingFunctionRegistry
    from lancedb.pydantic import LanceModel, Vector

    os.environ["JINA_API_KEY"] = "your-api-key"

    jina_embed = (
        EmbeddingFunctionRegistry.get_instance()
        .get("jina")
        .create(name="jina-embeddings-v2-base-en")
    )

    class TextModel(LanceModel):
        text: str = jina_embed.SourceField()
        vector: Vector(jina_embed.ndims()) = jina_embed.VectorField()

    data = [{"text": "hello world"}, {"text": "goodbye world"}]

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "jina-text"))
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)
    # --8<-- [end:embedding_jina_text]


def embedding_jina_multimodal():
    # --8<-- [start:embedding_jina_multimodal]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    import pandas as pd
    import requests
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    os.environ["JINA_API_KEY"] = "your-api-key"

    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "jina-images"))
    func = get_registry().get("jina").create()

    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()
        image_bytes: bytes = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()
        vec_from_bytes: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("images", schema=Images)
    labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
        "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
    ]
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
        pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )
    # --8<-- [end:embedding_jina_multimodal]


def reranking_cohere_usage():
    # --8<-- [start:reranking_cohere_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import CohereReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "cohere-rerank"))

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema)
    tbl.add(data)
    reranker = CohereReranker(api_key="your-api-key")

    # Run vector search with a reranker
    vector_results = tbl.search("hello").rerank(reranker=reranker).to_list()
    print(vector_results)

    # Build the full-text index before either FTS or hybrid search.
    tbl.create_fts_index("text", replace=True)

    # Run FTS search with a reranker
    fts_results = (
        tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()
    )
    print(fts_results)

    # Run hybrid search with a reranker
    hybrid_results = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    print(hybrid_results)
    # --8<-- [end:reranking_cohere_usage]


def reranking_jina_usage():
    # --8<-- [start:reranking_jina_usage]
    import os
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import JinaReranker

    os.environ["JINA_API_KEY"] = "your-api-key"
    embedder = get_registry().get("jina").create()
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "jina-rerank"))

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema)
    tbl.add(data)
    reranker = JinaReranker(api_key="your-api-key")

    # Run vector search with a reranker
    vector_results = tbl.search("hello").rerank(reranker=reranker).to_list()
    print(vector_results)

    # Build the full-text index before either FTS or hybrid search.
    tbl.create_fts_index("text", replace=True)

    # Run FTS search with a reranker
    fts_results = (
        tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()
    )
    print(fts_results)

    # Run hybrid search with a reranker
    hybrid_results = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    print(hybrid_results)
    # --8<-- [end:reranking_jina_usage]


def reranking_watsonx_usage():
    # --8<-- [start:reranking_watsonx_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import WatsonxReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "watsonx-rerank"))

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema)
    tbl.add(data)

    # Supply your API key and project ID (or use space_id instead of project_id).
    reranker = WatsonxReranker(
        api_key="your-api-key",
        project_id="your-project-id",
    )

    # Run vector search with a reranker
    vector_results = tbl.search("hello").rerank(reranker=reranker).to_list()
    print(vector_results)

    # Build the full-text index before either FTS or hybrid search.
    tbl.create_fts_index("text", replace=True)

    # Run FTS search with a reranker
    fts_results = (
        tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()
    )
    print(fts_results)

    # Run hybrid search with a reranker
    hybrid_results = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    print(hybrid_results)
    # --8<-- [end:reranking_watsonx_usage]


def reranking_openai_usage():
    # --8<-- [start:reranking_openai_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import OpenaiReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "openai-rerank"))

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema)
    tbl.add(data)
    reranker = OpenaiReranker(api_key="your-api-key")

    # Run vector search with a reranker
    vector_results = tbl.search("hello").rerank(reranker=reranker).to_list()
    print(vector_results)

    # Build the full-text index before either FTS or hybrid search.
    tbl.create_fts_index("text", replace=True)

    # Run FTS search with a reranker
    fts_results = (
        tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()
    )
    print(fts_results)

    # Run hybrid search with a reranker
    hybrid_results = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    print(hybrid_results)
    # --8<-- [end:reranking_openai_usage]


def reranking_voyageai_usage():
    # --8<-- [start:reranking_voyageai_usage]
    import tempfile
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.rerankers import VoyageAIReranker

    embedder = get_registry().get("sentence-transformers").create()
    db = lancedb.connect(str(Path(tempfile.mkdtemp()) / "voyageai-rerank"))

    class Schema(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()

    data = [
        {"text": "hello world"},
        {"text": "goodbye world"},
    ]
    tbl = db.create_table("test", schema=Schema)
    tbl.add(data)
    reranker = VoyageAIReranker(api_key="your-api-key", model_name="rerank-2")

    # Run vector search with a reranker
    vector_results = tbl.search("hello").rerank(reranker=reranker).to_list()
    print(vector_results)

    # Build the full-text index before either FTS or hybrid search.
    tbl.create_fts_index("text", replace=True)

    # Run FTS search with a reranker
    fts_results = (
        tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()
    )
    print(fts_results)

    # Run hybrid search with a reranker
    hybrid_results = (
        tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()
    )
    print(hybrid_results)
    # --8<-- [end:reranking_voyageai_usage]
