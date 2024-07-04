# Jina Reranker

This re-ranker uses the [Jina](https://jina.ai/reranker/) API to rerank the search results. You can use this re-ranker by passing `JinaReranker()` to the `rerank()` method. Note that you'll either need to set the `JINA_API_KEY` environment variable or pass the `api_key` argument to use this re-ranker.


!!! note
    Supported Query Types: Hybrid, Vector, FTS


```python
import os
import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import JinaReranker

os.environ['JINA_API_KEY'] = "jina_*"


embedder = get_registry().get("jina").create()
db = lancedb.connect("~/.lancedb")

class Schema(LanceModel):
    text: str = embedder.SourceField()
    vector: Vector(embedder.ndims()) = embedder.VectorField()

data = [
    {"text": "hello world"},
    {"text": "goodbye world"}
    ]
tbl = db.create_table("test", schema=Schema, mode="overwrite")
tbl.add(data)
reranker = JinaReranker(api_key="key")

# Run vector search with a reranker
result = tbl.search("hello").rerank(reranker=reranker).to_list() 

# Run FTS search with a reranker
result = tbl.search("hello", query_type="fts").rerank(reranker=reranker).to_list()

# Run hybrid search with a reranker
tbl.create_fts_index("text", replace=True)
result = tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()

```

Accepted Arguments
----------------
| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `model_name` | `str` | `"jina-reranker-v2-base-multilingual"` | The name of the reranker model to use. You can find the list of available models in https://jina.ai/reranker/|
| `column` | `str` | `"text"` | The name of the column to use as input to the cross encoder model. |
| `top_n` | `str` | `None` | The number of results to return. If None, will return all results. |
| `api_key` | `str` | `None` | The API key for the Jina API. If not provided, the `JINA_API_KEY` environment variable is used. |
| `return_score` | str | `"relevance"` | Options are "relevance" or "all". The type of score to return. If "relevance", will return only the `_relevance_score. If "all" is supported, will return relevance score along with the vector and/or fts scores depending on query type |



## Supported Scores for each query type
You can specify the type of scores you want the reranker to return. The following are the supported scores for each query type:

### Hybrid Search
|`return_score`| Status | Description |
| --- | --- | --- |
| `relevance` | ✅ Supported | Returns only have the `_relevance_score` column |
| `all` | ❌ Not Supported | Returns have vector(`_distance`) and FTS(`score`) along with Hybrid Search score(`_relevance_score`) |

### Vector Search
|`return_score`| Status | Description |
| --- | --- | --- |
| `relevance` | ✅ Supported | Returns only have the `_relevance_score` column |
| `all` | ✅ Supported | Returns have vector(`_distance`) along with Hybrid Search score(`_relevance_score`) |

### FTS Search
|`return_score`| Status | Description |
| --- | --- | --- |
| `relevance` | ✅ Supported | Returns only have the `_relevance_score` column |
| `all` | ✅ Supported | Returns have FTS(`score`) along with Hybrid Search score(`_relevance_score`) |