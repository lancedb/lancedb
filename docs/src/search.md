# Vector Search

`Vector Search` finds the nearest vectors from the database.
In a recommendation system or search engine, you can find similar products from
the one you searched.
In LLM and other AI applications,
each data point can be [presented by the embeddings generated from some models](embeddings/index.md),
it returns the most relevant features.

A search in high-dimensional vector space, is to find `K-Nearest-Neighbors (KNN)` of the query vector.

## Metric

In LanceDB, a `Metric` is the way to describe the distance between a pair of vectors.
Currently, we support the following metrics:

| Metric      | Description                          |
| ----------- | ------------------------------------ |
| `L2`        | [Euclidean / L2 distance](https://en.wikipedia.org/wiki/Euclidean_distance) |
| `Cosine`    | [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity)|
| `Dot`       | [Dot Production](https://en.wikipedia.org/wiki/Dot_product) |


## Search

### Flat Search

If you do not create a vector index, LanceDB would need to exhaustively scan the entire vector column (via `Flat Search`)
and compute the distance for *every* vector in order to find the closest matches. This is effectively a KNN search.


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
<!-- Setup Code
```javascript
const vectordb_setup = require('vectordb')
const db_setup = await vectordb_setup.connect('data/sample-lancedb')

let data = []
for (let i = 0; i < 10_000; i++) {
     data.push({vector: Array(1536).fill(i), id: `${i}`, content: "", longId: `${i}`},)
}
await db_setup.createTable('my_vectors', data)
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

=== "JavaScript"

    ```javascript
    const vectordb = require('vectordb')
    const db = await vectordb.connect('data/sample-lancedb')

    const tbl = await db.openTable("my_vectors")

    const results_1 = await tbl.search(Array(1536).fill(1.2))
        .limit(10)
        .execute()
    ```

By default, `l2` will be used as `Metric` type. You can customize the metric type
as well.

=== "Python"

    ```python
    df = tbl.search(np.random.random((1536))) \
        .metric("cosine") \
        .limit(10) \
        .to_list()
    ```


=== "JavaScript"

    ```javascript
    const results_2 = await tbl.search(Array(1536).fill(1.2))
        .metricType("cosine")
        .limit(10)
        .execute()
    ```


### Approximate Nearest Neighbor (ANN) Search with Vector Index.

To accelerate vector retrievals, it is common to build vector indices.
A vector index is a data structure specifically designed to efficiently organize and
search vector data based on their similarity via the chosen distance metric.
By constructing a vector index, you can reduce the search space and avoid the need
for brute-force scanning of the entire vector column.

However, fast vector search using indices often entails making a trade-off with accuracy to some extent.
This is why it is often called **Approximate Nearest Neighbors (ANN)** search, while the Flat Search (KNN)
always returns 100% recall.

See [ANN Index](ann_indexes.md) for more details.


### Output formats

LanceDB returns results in many different formats commonly used in python.
Let's create a LanceDB table with a nested schema:

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
        content=f"document{i}", meta=Metadata(source=f"source{i%10}", timestamp=datetime.now())
    ),
) for i in range(100)]

tbl = db.create_table("documents", data=data)
```

#### As a pyarrow table

Using `to_arrow()` we can get the results back as a pyarrow Table.
This result table has the same columns as the LanceDB table, with 
the addition of an `_distance` column for vector search or a `score`
column for full text search.

```python
tbl.search(np.random.randn(1536)).to_arrow()
```

#### As a pandas dataframe

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


#### As a list of python dicts

You can of course return results as a list of python dicts.

```python
tbl.search(np.random.randn(1536)).to_list()
```

#### As a list of pydantic models

We can add data using pydantic models, and we can certainly
retrieve results as pydantic models

```python
tbl.search(np.random.randn(1536)).to_pydantic(LanceSchema)
```

Note that in this case the extra `_distance` field is discarded since
it's not part of the LanceSchema.

