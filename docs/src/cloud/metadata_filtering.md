LanceDB Cloud supports rich filtering features of query results based on metadata fields. 

By default, _post-filtering_ is performed on the top-k results returned by the vector search. 
However, _pre-filtering_ is also an option that performs the filter prior to vector search. 
This can be useful to narrow down on the search space on a very large dataset to reduce query 
latency.

=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:filtering"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"

    --8<-- "nodejs/examples/cloud.test.ts:filtering"
    ```
We also support standard SQL expressions as predicates for filtering operations. 
It can be used during vector search, update, and deletion operations.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:sql_filtering"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    
    --8<-- "nodejs/examples/cloud.test.ts:sql_filtering"
    ```