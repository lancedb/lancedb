# Hybrid Search

LanceDB supports both semantic and keyword-based search (also termed full-text search, or FTS). In real world applications, it is often useful to combine these two approaches to get the best best results. For example, you may want to search for a document that is semantically similar to a query document, but also contains a specific keyword. This is an example of *hybrid search*, a search algorithm that combines multiple search techniques.

## Hybrid search in LanceDB
You can perform hybrid search in LanceDB by combining the results of semantic and full-text search via a reranking algorithm of your choice. LanceDB provides multiple rerankers out of the box. However, you can always write a custom reranker if your use case need more sophisticated logic .

```python
import os

import lancedb
import openai
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector

db = lancedb.connect("~/.lancedb")

# Ingest embedding function in LanceDB table
# Configuring the environment variable OPENAI_API_KEY
if "OPENAI_API_KEY" not in os.environ:
# OR set the key here as a variable
    openai.api_key = "sk-..."
embeddings = get_registry().get("openai").create()

class Documents(LanceModel):
    vector: Vector(embeddings.ndims()) = embeddings.VectorField()
    text: str = embeddings.SourceField()

table = db.create_table("documents", schema=Documents)

data = [
    { "text": "rebel spaceships striking from a hidden base"},
    { "text": "have won their first victory against the evil Galactic Empire"},
    { "text": "during the battle rebel spies managed to steal secret plans"},
    { "text": "to the Empire's ultimate weapon the Death Star"}
]

# ingest docs with auto-vectorization
table.add(data)

# Create a fts index before the hybrid search
table.create_fts_index("text")
# hybrid search with default re-ranker
results = table.search("flower moon", query_type="hybrid").to_pandas()
```

By default, LanceDB uses `LinearCombinationReranker(weight=0.7)` to combine and rerank the results of semantic and full-text search. You can customize the hyperparameters as needed or write your own custom reranker. Here's how you can use any of the available rerankers:


### `rerank()` arguments
* `normalize`: `str`, default `"score"`:
    The method to normalize the scores. Can be "rank" or "score". If "rank", the scores are converted to ranks and then normalized. If "score", the scores are normalized directly.
* `reranker`: `Reranker`, default `LinearCombinationReranker(weight=0.7)`.
    The reranker to use. If not specified, the default reranker is used.


## Available Rerankers
LanceDB provides a number of re-rankers out of the box. You can use any of these re-rankers by passing them to the `rerank()` method. Visit the [rerankers](../reranking/) page for more information on each re-ranker.

## Custom Rerankers
You can also create custom rerankers by extending the base `Reranker` class. The custom reranker should implement the `rerank` method that takes a list of search results and returns a reranked list of search results. Visit the [custom rerankers](../reranking/custom_reranker.md) page for more information on creating custom rerankers.
