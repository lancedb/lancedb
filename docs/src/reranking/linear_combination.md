# Linear Combination Reranker

!!! note
    This is depricated. It is recommended to use the `RRFReranker` instead, if you want to use a score based reranker.

It combines the results of semantic and full-text search using a linear combination of the scores. The weights for the linear combination can be specified. It defaults to 0.7, i.e, 70% weight for semantic search and 30% weight for full-text search.

!!! note
    Supported Query Types: Hybrid


```python
import numpy
import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import LinearCombinationReranker

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
reranker = LinearCombinationReranker()

# Run hybrid search with a reranker
tbl.create_fts_index("text", replace=True)
result = tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()

```

Accepted Arguments
----------------
| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `weight` | `float` | `0.7` | The weight to use for the semantic search score. The weight for the full-text search score is `1 - weights`. |
| `return_score` | str | `"relevance"` | Options are "relevance" or "all". The type of score to return. If "relevance", will return only the `_relevance_score. If "all", will return all scores from the vector and FTS search along with the relevance score. |


## Supported Scores for each query type
You can specify the type of scores you want the reranker to return. The following are the supported scores for each query type:

### Hybrid Search
|`return_score`| Status | Description |
| --- | --- | --- |
| `relevance` | ✅ Supported | Returns only have the `_relevance_score` column |
| `all` | ✅ Supported | Returns have vector(`_distance`) and FTS(`score`) along with Hybrid Search score(`_distance`) |