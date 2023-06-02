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