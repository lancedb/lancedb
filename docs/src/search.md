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

| Metric   | Description                                                                 |
| -------- | --------------------------------------------------------------------------- |
| `l2`     | [Euclidean / L2 distance](https://en.wikipedia.org/wiki/Euclidean_distance) |
| `cosine` | [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity)        |
| `dot`    | [Dot Production](https://en.wikipedia.org/wiki/Dot_product)                 |

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

    ```python
    import lancedb
    import numpy as np

    db = lancedb.connect("data/sample-lancedb")

    tbl = db.open_table("my_vectors")

    df = tbl.search(np.random.random((1536))) \
        .limit(10) \
        .to_list()
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/search.ts:import"

        --8<-- "nodejs/examples/search.ts:search1"
        ```


    === "vectordb (deprecated)"

        ```ts
        --8<-- "docs/src/search_legacy.ts:import"

        --8<-- "docs/src/search_legacy.ts:search1"
        ```

By default, `l2` will be used as metric type. You can specify the metric type as
`cosine` or `dot` if required.

=== "Python"

    ```python
    df = tbl.search(np.random.random((1536))) \
        .metric("cosine") \
        .limit(10) \
        .to_list()
    ```

=== "TypeScript"

    === "@lancedb/lancedb"

        ```ts
        --8<-- "nodejs/examples/search.ts:search2"
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

## Output search results

LanceDB returns vector search results via different formats commonly used in python.
Let's create a LanceDB table with a nested schema:

=== "Python"

    ```python

    from datetime import datetime
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    import numpy as np
    from pydantic import BaseModel
    uri = "data/sample-lancedb-nested"

    class Metadata(BaseModel):
        source: str
        timestamp: datetime

    class Document(BaseModel):
        content: str
        meta: Metadata

    class LanceSchema(LanceModel):
        id: str
        vector: Vector(1536)
        payload: Document

    # Let's add 100 sample rows to our dataset
    data = [LanceSchema(
        id=f"id{i}",
        vector=np.random.randn(1536),
        payload=Document(
            content=f"document{i}", meta=Metadata(source=f"source{i % 10}", timestamp=datetime.now())
        ),
    ) for i in range(100)]

    tbl = db.create_table("documents", data=data)
    ```

    ### As a PyArrow table

    Using `to_arrow()` we can get the results back as a pyarrow Table.
    This result table has the same columns as the LanceDB table, with
    the addition of an `_distance` column for vector search or a `score`
    column for full text search.

    ```python
    tbl.search(np.random.randn(1536)).to_arrow()
    ```

    ### As a Pandas DataFrame

    You can also get the results as a pandas dataframe.

    ```python
    tbl.search(np.random.randn(1536)).to_pandas()
    ```

    While other formats like Arrow/Pydantic/Python dicts have a natural
    way to handle nested schemas, pandas can only store nested data as a
    python dict column, which makes it difficult to support nested references.
    So for convenience, you can also tell LanceDB to flatten a nested schema
    when creating the pandas dataframe.

    ```python
    tbl.search(np.random.randn(1536)).to_pandas(flatten=True)
    ```

    If your table has a deeply nested struct, you can control how many levels
    of nesting to flatten by passing in a positive integer.

    ```python
    tbl.search(np.random.randn(1536)).to_pandas(flatten=1)
    ```

    ### As a list of Python dicts

    You can of course return results as a list of python dicts.

    ```python
    tbl.search(np.random.randn(1536)).to_list()
    ```

    ### As a list of Pydantic models

    We can add data using Pydantic models, and we can certainly
    retrieve results as Pydantic models

    ```python
    tbl.search(np.random.randn(1536)).to_pydantic(LanceSchema)
    ```

    Note that in this case the extra `_distance` field is discarded since
    it's not part of the LanceSchema.
