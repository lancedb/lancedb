# Vector Search

A vector search finds the approximate or exact nearest neighbors to a given query vector.

- In a recommendation system or search engine, you can find similar records to
  the one you searched.
- In LLM and other AI applications,
  each data point can be represented by [embeddings generated from existing models](embeddings/index.md),
  following which the search returns the most relevant features.

## Distance metrics

Distance metrics are a measure of the similarity between a pair of vectors.
Currently, LanceDB supports the following metrics:

| Metric    | Description                                                                 |
| --------- | --------------------------------------------------------------------------- |
| `l2`      | [Euclidean / l2 distance](https://en.wikipedia.org/wiki/Euclidean_distance) |
| `cosine`  | [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity)        |
| `dot`     | [Dot Production](https://en.wikipedia.org/wiki/Dot_product)                 |
| `hamming` | [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance)          |

!!! note
    The `hamming` metric is only available for binary vectors.

## Exhaustive search (kNN)

If you do not create a vector index, LanceDB exhaustively scans the _entire_ vector space
and computes the distance to every vector in order to find the exact nearest neighbors. This is effectively a kNN search.

<!-- Setup Code
```python
import lancedb
import numpy as np
uri = "data/sample-lancedb"
db = lancedb.connect(uri)

data = [{"vector": row, "item": f"item {i}"}
     for i, row in enumerate(np.random.random((10_000, 1536)).astype('float32'))]

db.create_table("my_vectors", data=data)
```
-->

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:exhaustive_search"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:exhaustive_search_async"
        ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/search.test.ts:import"

        --8<-- "nodejs/examples/search.test.ts:search1"
        ```


    === "vectordb (deprecated)"

        ```ts
        --8<-- "docs/src/search_legacy.ts:import"

        --8<-- "docs/src/search_legacy.ts:search1"
        ```

By default, `l2` will be used as metric type. You can specify the metric type as
`cosine` or `dot` if required.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:exhaustive_search_cosine"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:exhaustive_search_async_cosine"
        ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/search.test.ts:search2"
        ```

    === "vectordb (deprecated)"

        ```javascript
        --8<-- "docs/src/search_legacy.ts:search2"
        ```

## Approximate nearest neighbor (ANN) search

To perform scalable vector retrieval with acceptable latencies, it's common to build a vector index.
While the exhaustive search is guaranteed to always return 100% recall, the approximate nature of
an ANN search means that using an index often involves a trade-off between recall and latency.

See the [IVF_PQ index](./concepts/index_ivfpq.md) for a deeper description of how `IVF_PQ`
indexes work in LanceDB.

## Binary vector

LanceDB supports binary vectors as a data type, and has the ability to search binary vectors with hamming distance. The binary vectors are stored as uint8 arrays (every 8 bits are stored as a byte):

!!! note
    The dim of the binary vector must be a multiple of 8. A vector of dim 128 will be stored as a uint8 array of size 16.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_binary_vector.py:imports"

        --8<-- "python/python/tests/docs/test_binary_vector.py:sync_binary_vector"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_binary_vector.py:imports"

        --8<-- "python/python/tests/docs/test_binary_vector.py:async_binary_vector"
        ```

## Multivector type

LanceDB supports multivector type, this is useful when you have multiple vectors for a single item (e.g. with ColBert and ColPali).

You can index on a column with multivector type and search on it, the query can be single vector or multiple vectors. If the query is multiple vectors `mq`, the similarity (distance) from it to any multivector `mv` in the dataset, is defined as:

![maxsim](assets/maxsim.png)

where `sim` is the similarity function (e.g. cosine).

For now, only `cosine` metric is supported for multivector search.
The vector value type can be `float16`, `float32` or `float64`.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_multivector.py:imports"

        --8<-- "python/python/tests/docs/test_multivector.py:sync_multivector"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_multivector.py:imports"

        --8<-- "python/python/tests/docs/test_multivector.py:async_multivector"
        ```

## Search with distance range

You can also search for vectors within a specific distance range from the query vector. This is useful when you want to find vectors that are not just the nearest neighbors, but also those that are within a certain distance. This can be done by using the `distance_range` method.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_distance_range.py:imports"

        --8<-- "python/python/tests/docs/test_distance_range.py:sync_distance_range"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_distance_range.py:imports"

        --8<-- "python/python/tests/docs/test_distance_range.py:async_distance_range"
        ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/search.test.ts:import"

        --8<-- "nodejs/examples/search.test.ts:distance_range"
        ```


## Output search results

LanceDB returns vector search results via different formats commonly used in python.
Let's create a LanceDB table with a nested schema:

=== "Python"
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:import-datetime"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb-pydantic"
        --8<-- "python/python/tests/docs/test_search.py:import-numpy"
        --8<-- "python/python/tests/docs/test_search.py:import-pydantic-base-model"
        --8<-- "python/python/tests/docs/test_search.py:class-definition"
        --8<-- "python/python/tests/docs/test_search.py:create_table_with_nested_schema"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:import-datetime"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb-pydantic"
        --8<-- "python/python/tests/docs/test_search.py:import-numpy"
        --8<-- "python/python/tests/docs/test_search.py:import-pydantic-base-model"
        --8<-- "python/python/tests/docs/test_search.py:class-definition"
        --8<-- "python/python/tests/docs/test_search.py:create_table_async_with_nested_schema"
        ```

    ### As a PyArrow table

    Using `to_arrow()` we can get the results back as a pyarrow Table.
    This result table has the same columns as the LanceDB table, with
    the addition of an `_distance` column for vector search or a `score`
    column for full text search.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_pyarrow"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_async_as_pyarrow"
        ```

    ### As a Pandas DataFrame

    You can also get the results as a pandas dataframe.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_pandas"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_async_as_pandas"
        ```

    While other formats like Arrow/Pydantic/Python dicts have a natural
    way to handle nested schemas, pandas can only store nested data as a
    python dict column, which makes it difficult to support nested references.
    So for convenience, you can also tell LanceDB to flatten a nested schema
    when creating the pandas dataframe.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_pandas_flatten_true"
        ```

    If your table has a deeply nested struct, you can control how many levels
    of nesting to flatten by passing in a positive integer.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_pandas_flatten_1"
        ```
    !!! note
        `flatten` is not yet supported with our asynchronous client.

    ### As a list of Python dicts

    You can of course return results as a list of python dicts.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_list"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_async_as_list"
        ```

    ### As a list of Pydantic models

    We can add data using Pydantic models, and we can certainly
    retrieve results as Pydantic models

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:search_result_as_pydantic"
        ```
    !!! note
        `to_pydantic()` is not yet supported with our asynchronous client.

    Note that in this case the extra `_distance` field is discarded since
    it's not part of the LanceSchema.
