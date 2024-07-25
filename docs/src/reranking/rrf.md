# Reciprocal Rank Fusion Reranker

Reciprocal Rank Fusion (RRF) is an algorithm that evaluates the search scores by leveraging the positions/rank of the documents. The implementation follows this [paper](https://plg.uwaterloo.ca/~gvcormac/cormacksigir09-rrf.pdf).


!!! note
    Supported Query Types: Hybrid


```python
import numpy
import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import RRFReranker

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
reranker = RRFReranker()

# Run hybrid search with a reranker
tbl.create_fts_index("text", replace=True)
result = tbl.search("hello", query_type="hybrid").rerank(reranker=reranker).to_list()

```

Accepted Arguments
----------------
| Argument | Type | Default | Description |
| --- | --- | --- | --- |
| `K` | `int` | `60` | A constant used in the RRF formula (default is 60). Experiments indicate that k = 60 was near-optimal, but that the choice is not critical |
| `return_score` | str | `"relevance"` | Options are "relevance" or "all". The type of score to return. If "relevance", will return only the `_relevance_score`. If "all", will return all scores from the vector and FTS search along with the relevance score. |


## Supported Scores for each query type
You can specify the type of scores you want the reranker to return. The following are the supported scores for each query type:

### Hybrid Search
|`return_score`| Status | Description |
| --- | --- | --- |
| `relevance` | ✅ Supported | Returned rows only have the `_relevance_score` column |
| `all` | ✅ Supported | Returned rows have vector(`_distance`) and FTS(`score`) along with Hybrid Search score(`_relevance_score`) |