# Vector Search

`Vector Search` finds the nearest vectors from the database.
In a recommendation system or search engine, you can find similar products from
the one you searched.
In LLM and other AI applications,
each data point can be [presented by the embeddings generated from some models](embedding.md),
it returns the most relevant features.

A search in high-dimensional vector space, is to find `K-Nearest-Neighbors (KNN)` of the query vector.

## Metric

In LanceDB, a `Metric` is the way to describe the distance between a pair of vectors.
Currently, we support the following metrics:

| Metric      | Description                          |
| ----------- | ------------------------------------ |
| `L2`        | [Euclidean / L2 distance](https://en.wikipedia.org/wiki/Euclidean_distance) |
| `Cosine`    | [Cosine Similarity](https://en.wikipedia.org/wiki/Cosine_similarity)|


## Search

### Flat Search


If there is no [vector index is created](ann_indexes.md), LanceDB will just brute-force scan
the vector column and compute the distance.

=== "Python"

    ```python
    import lancedb
    db = lancedb.connect("data/sample-lancedb")

    tbl = db.open_table("my_vectors")

    df = tbl.search(np.random.random((768)))
        .limit(10)
        .to_df()
    ```

=== "JavaScript"

    ```javascript
    const vectordb = require('vectordb')
    const db = await vectordb.connect('data/sample-lancedb')

    tbl = db.open_table("my_vectors")

    const results = await tbl.search(Array(768))
        .limit(20)
        .execute()
    ```

By default, `l2` will be used as `Metric` type. You can customize the metric type
as well.

=== "Python"

    ```python
    df = tbl.search(np.random.random((768)))
        .metric("cosine")
        .limit(10)
        .to_df()
    ```

=== "JavaScript"

    ```javascript
    const vectordb = require('vectordb')
    const db = await vectordb.connect('data/sample-lancedb')

    tbl = db.open_table("my_vectors")

    const results = await tbl.search(Array(768))
        .metric("cosine")
        .limit(20)
        .execute()
    ```

### Search with Vector Index.

See [ANN Index](ann_indexes.md) for more details.