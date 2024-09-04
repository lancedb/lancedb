# AnswersDotAI Rerankers

This integration allows using answersdotai's rerankers to rerank the search results. [Rerankers](https://github.com/AnswerDotAI/rerankers)
A lightweight, low-dependency, unified API to use all common reranking and cross-encoder models.

!!! note
    Supported Query Types: Hybrid, Vector, FTS


```python
import numpy
import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import AnswerdotaiRerankers

embedder = get_registry().get("sentence-transformers").create()
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
reranker = AnswerdotaiRerankers()

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
| `model_type` | `str` | `"colbert"` | The type of model to use. Supported model types can be found here - https://github.com/AnswerDotAI/rerankers |
| `model_name` | `str` | `"answerdotai/answerai-colbert-small-v1"` | The name of the reranker model to use. |
| `column` | `str` | `"text"` | The name of the column to use as input to the cross encoder model. |
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