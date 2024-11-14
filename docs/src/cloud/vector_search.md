Users can also tune the following parameters for better search quality.

 - [nprobes](https://lancedb.github.io/lancedb/js/classes/VectorQuery/#nprobes): 
 the number of partitions to search (probe).
 - [refine factor](https://lancedb.github.io/lancedb/js/classes/VectorQuery/#refinefactor): 
 a multiplier to control how many additional rows are taken during the refine step.

 [Metadata filtering](filtering) combined with the vector search is also supported.

=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:vector_search"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    
    --8<-- "nodejs/examples/cloud.test.ts:vector_search"
    ```