LanceDB Cloud supports **vector index**, **scalar index** and **full-text search index**. Compared to open-source version, LanceDB Cloud focuses on **automation**:

- If there is a single vector column in the table, the vector column can be inferred from the schema and the index will be automatically created. 

- Indexing parameters will be automatically tuned for customer's data.

## Vector index
LanceDB has implemented the state-of-art indexing algorithms (more about [IVF-PQ](https://lancedb.github.io/lancedb/concepts/index_ivfpq/) and [HNSW](https://lancedb.github.io/lancedb/concepts/index_hnsw/)). We currently 
support the _L2_, _Cosine_ and _Dot_ as distance calculation metrics. You can create multiple vector indices within a table.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:create_index"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"

    --8<-- "nodejs/examples/cloud.test.ts:connect_db_and_open_table"
    --8<-- "nodejs/examples/cloud.test.ts:create_index"
    ```

## Scalar index
LanceDB Cloud and LanceDB Enterprise supports several types of Scalar indices to accelerate search over scalar columns.

- *BTREE*: The most common type is BTREE. This index is inspired by the btree data structure although only the first few layers of the btree are cached in memory. It will perform well on columns with a large number of unique values and few rows per value.
- *BITMAP*: this index stores a bitmap for each unique value in the column. This index is useful for columns with a finite number of unique values and many rows per value. 
    - For example, columns that represent "categories", "labels", or "tags"
- *LABEL_LIST*: a special index that is used to index list columns whose values have a finite set of possibilities. 
    - For example, a column that contains lists of tags (e.g. ["tag1", "tag2", "tag3"]) can be indexed with a LABEL_LIST index.

You can create multiple scalar indices within a table.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:create_scalar_index"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    
    --8<-- "nodejs/examples/cloud.test.ts:connect_db_and_open_table"
    --8<-- "nodejs/examples/cloud.test.ts:create_scalar_index"
    ```

## Full-text search index
We provide performant full-text search on LanceDB Cloud, allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions.
!!! note ""

    `use_tantivy` is not available with `create_fts_index` on LanceDB Cloud as we used our native implementation, which has better performance comparing to tantivy.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:create_fts_index"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    
    --8<-- "nodejs/examples/cloud.test.ts:create_fts_index"
    ```