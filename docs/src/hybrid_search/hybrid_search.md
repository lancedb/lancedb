# Hybrid Search

LanceDB supports both semantic and keyword-based search (also termed full-text search, or FTS). In real world applications, it is often useful to combine these two approaches to get the best best results. For example, you may want to search for a document that is semantically similar to a query document, but also contains a specific keyword. This is an example of *hybrid search*, a search algorithm that combines multiple search techniques.

## Hybrid search in LanceDB
You can perform hybrid search in LanceDB by combining the results of semantic and full-text search via a reranking algorithm of your choice. LanceDB provides multiple rerankers out of the box. However, you can always write a custom reranker if your use case need more sophisticated logic .

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:import-os"
    --8<-- "python/python/tests/docs/test_search.py:import-openai"
    --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
    --8<-- "python/python/tests/docs/test_search.py:import-embeddings"
    --8<-- "python/python/tests/docs/test_search.py:import-pydantic"
    --8<-- "python/python/tests/docs/test_search.py:import-lancedb-fts"
    --8<-- "python/python/tests/docs/test_search.py:import-openai-embeddings"
    --8<-- "python/python/tests/docs/test_search.py:class-Documents"
    --8<-- "python/python/tests/docs/test_search.py:basic_hybrid_search"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:import-os"
    --8<-- "python/python/tests/docs/test_search.py:import-openai"
    --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
    --8<-- "python/python/tests/docs/test_search.py:import-embeddings"
    --8<-- "python/python/tests/docs/test_search.py:import-pydantic"
    --8<-- "python/python/tests/docs/test_search.py:import-lancedb-fts"
    --8<-- "python/python/tests/docs/test_search.py:import-openai-embeddings"
    --8<-- "python/python/tests/docs/test_search.py:class-Documents"
    --8<-- "python/python/tests/docs/test_search.py:basic_hybrid_search_async"
    ```
    
!!! Note
    You can also pass the vector and text query manually. This is useful if you're not using the embedding API or if you're using a separate embedder service.
### Explicitly passing the vector and text query
=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:hybrid_search_pass_vector_text"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:hybrid_search_pass_vector_text_async"
    ```

By default, LanceDB uses `RRFReranker()`, which uses reciprocal rank fusion score, to combine and rerank the results of semantic and full-text search. You can customize the hyperparameters as needed or write your own custom reranker. Here's how you can use any of the available rerankers:


### `rerank()` arguments
* `normalize`: `str`, default `"score"`:
    The method to normalize the scores. Can be "rank" or "score". If "rank", the scores are converted to ranks and then normalized. If "score", the scores are normalized directly.
* `reranker`: `Reranker`, default `RRF()`.
    The reranker to use. If not specified, the default reranker is used.


## Available Rerankers
LanceDB provides a number of re-rankers out of the box. You can use any of these re-rankers by passing them to the `rerank()` method. 
Go to [Rerankers](../reranking/index.md) to learn more about using the available rerankers and implementing custom rerankers.


