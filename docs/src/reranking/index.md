Reranking is the process of reordering a list of items based on some criteria. In the context of search, reranking is used to reorder the search results returned by a search engine based on some criteria. This can be useful when the initial ranking of the search results is not satisfactory or when the user has provided additional information that can be used to improve the ranking of the search results.

LanceDB comes with some built-in rerankers. Some of the rerankers that are available in LanceDB are:

| Reranker | Description | Supported Query Types |
| --- | --- | --- |
| `LinearCombinationReranker` | Reranks search results based on a linear combination of FTS and vector search scores | Hybrid |
| `CohereReranker` | Uses cohere Reranker API to rerank results | Vector, FTS, Hybrid |
| `CrossEncoderReranker` | Uses a cross-encoder model to rerank search results | Vector, FTS, Hybrid |
| `ColbertReranker` | Uses a colbert model to rerank search results | Vector, FTS, Hybrid |
| `OpenaiReranker`(Experimental) | Uses OpenAI's chat model to rerank search results | Vector, FTS, Hybrid |


## Using a Reranker
Using rerankers is optional for vector and FTS. However, for hybrid search, rerankers are required. To use a reranker, you need to create an instance of the reranker and pass it to the `rerank` method of the query builder.

```python
import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
from lancedb.rerankers import CohereReranker

embedder = get_registry().get("sentence-transformers").create()
db = lancedb.connect("~/.lancedb")

class Schema(LanceModel):
    text: str = embedder.SourceField()
    vector: Vector(embedder.ndims()) = embedder.VectorField()

data = [
    {"text": "hello world"},
    {"text": "goodbye world"}
    ]
tbl = db.create_table("test", data)
reranker = CohereReranker(api_key="your_api_key")

# Run vector search with a reranker
result = tbl.query("hello").rerank(reranker).to_list() 

# Run FTS search with a reranker
result = tbl.query("hello", query_type="fts").rerank(reranker).to_list()

# Run hybrid search with a reranker
tbl.create_fts_index("text")
result = tbl.query("hello", query_type="hybrid").rerank(reranker).to_list()
```

### Multi-vector reranking
Most rerankers support reranking based on multiple vectors. To rerank based on multiple vectors, you can pass a list of vectors to the `rerank` method. Here's an example of how to rerank based on multiple vector columns using the `CrossEncoderReranker`:

```python
from lancedb.rerankers import CrossEncoderReranker

reranker = CrossEncoderReranker()

query = "hello"

res1 = table.search(query, vector_column_name="vector").limit(3)
res2 = table.search(query, vector_column_name="text_vector").limit(3)
res3 = table.search(query, vector_column_name="meta_vector").limit(3)

reranked = reranker.rerank_multivector([res1, res2, res3],  deduplicate=True)
```
    
## Available Rerankers
LanceDB comes with some built-in rerankers. Here are some of the rerankers that are available in LanceDB:

- [Cohere Reranker](./cohere.md)
- [Cross Encoder Reranker](./cross_encoder.md)
- [ColBERT Reranker](./colbert.md)
- [OpenAI Reranker](./openai.md)
- [Linear Combination Reranker](./linear_combination.md)
- [Jina Reranker](./jina.md)

## Creating Custom Rerankers

LanceDB also you to create custom rerankers by extending the base `Reranker` class. The custom reranker should implement the `rerank` method that takes a list of search results and returns a reranked list of search results. This is covered in more detail in the [Creating Custom Rerankers](./custom_reranker.md) section.