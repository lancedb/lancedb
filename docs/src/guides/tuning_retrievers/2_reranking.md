Continuing from the previous section, we can now rerank the results using more complex rerankers.

Try it yourself - <a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/lancedb_reranking.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>

## Reranking search results
You can rerank any search results using a reranker. The syntax for reranking is as follows:

```python
from lancedb.rerankers import LinearCombinationReranker

reranker = LinearCombinationReranker()
table.search(quries[0], query_type="hybrid").rerank(reranker=reranker).limit(5).to_pandas()
```
Based on the `query_type`, the `rerank()` function can accept other arguments as well. For example, hybrid search accepts a `normalize` param to determine the score normalization method.

!!! note "Note"
    LanceDB provides a `Reranker` base class that can be extended to implement custom rerankers. Each reranker must implement the `rerank_hybrid` method. `rerank_vector` and `rerank_fts` methods are optional. For example, the `LinearCombinationReranker` only implements the `rerank_hybrid` method and so it can only be used for reranking hybrid search results.

## Choosing a Reranker
There are many rerankers available in LanceDB like `CrossEncoderReranker`, `CohereReranker`, and `ColBERT`. The choice of reranker depends on the dataset and the application. You can even implement you own custom reranker by extending the `Reranker` class. For more details about each available reranker and performance comparison, refer to the [rerankers](https://lancedb.github.io/lancedb/reranking/) documentation.

In this example, we'll use the `CohereReranker` to rerank the search results. It requires  `cohere` to be installed and `COHERE_API_KEY` to be set in the environment. To get your API key, sign up on [Cohere](https://cohere.ai/).

```python
from lancedb.rerankers import CohereReranker

# use Cohere reranker v3
reranker = CohereReranker(model_name="rerank-english-v3.0") # default model is "rerank-english-v2.0"
```

### Reranking search results
Now we can rerank all query type results using the `CohereReranker`:

```python

# rerank hybrid search results
table.search(quries[0], query_type="hybrid").rerank(reranker=reranker).limit(5).to_pandas()

# rerank vector search results
table.search(quries[0], query_type="vector").rerank(reranker=reranker).limit(5).to_pandas()

# rerank fts search results
table.search(quries[0], query_type="fts").rerank(reranker=reranker).limit(5).to_pandas()
```

Each reranker can accept additional arguments. For example, `CohereReranker` accepts `top_k` and `batch_size` params to control the number of documents to rerank and the batch size for reranking respectively. Similarly, a custom reranker can accept any number of arguments based on the implementation. For example, a reranker can accept a `filter` that implements some custom logic to filter out documents before reranking.

## Results

Let us take a look at the same datasets from the previous sections, using the same embedding table but with Cohere reranker applied to all query types.

!!! note "Note"
    When reranking fts or vector search results, the search results are over-fetched by a factor of 2 and then reranked. From the reranked set, `top_k` (5 in this case) results are taken. This is done because reranking will have no effect on the hit-rate if we only fetch the `top_k` results.

### Synthetic LLama2 paper dataset

| Query Type | Hit-rate@5 |
| --- | --- |
| Vector |  0.640 |
| FTS   |  0.595  |
| Reranked vector | 0.677    |
| Reranked fts  | 0.672    |
| Hybrid | 0.759 |

### SQuAD Dataset


### Uber10K sec filing Dataset

| Query Type | Hit-rate@5 |
| --- | --- |
| Vector |  0.608 |
| FTS   |  0.824  |
| Reranked vector | 0.671    |
| Reranked fts  | 0.843    |
| Hybrid | 0.849 |




