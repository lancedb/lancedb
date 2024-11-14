We support hybrid search that combines semantic and full-text search via a 
reranking algorithm of your choice, to get the best of both worlds. LanceDB 
comes with [built-in rerankers](https://lancedb.github.io/lancedb/reranking/) 
and you can implement you own _customized reranker_ as well. 

=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:hybrid_search"
    ```