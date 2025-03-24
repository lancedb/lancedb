[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / HnswSqOptions

# Interface: HnswSqOptions

Options to create an `HNSW_SQ` index

## Properties

### distanceType?

```ts
optional distanceType: "l2" | "cosine" | "dot";
```

The distance metric used to train the index.

Default value is "l2".

The following distance types are available:

"l2" - Euclidean distance. This is a very common distance metric that
accounts for both magnitude and direction when determining the distance
between vectors. l2 distance has a range of [0, ∞).

"cosine" - Cosine distance.  Cosine distance is a distance metric
calculated from the cosine similarity between two vectors. Cosine
similarity is a measure of similarity between two non-zero vectors of an
inner product space. It is defined to equal the cosine of the angle
between them.  Unlike l2, the cosine distance is not affected by the
magnitude of the vectors.  Cosine distance has a range of [0, 2].

"dot" - Dot product. Dot distance is the dot product of two vectors. Dot
distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
l2 norm is 1), then dot distance is equivalent to the cosine distance.

***

### efConstruction?

```ts
optional efConstruction: number;
```

The number of candidates to evaluate during the construction of the HNSW graph.

The default value is 300.

This value controls the tradeoff between build speed and accuracy.
The higher the value the more accurate the build but the slower it will be.
150 to 300 is the typical range. 100 is a minimum for good quality search
results. In most cases, there is no benefit to setting this higher than 500.
This value should be set to a value that is not less than `ef` in the search phase.

***

### m?

```ts
optional m: number;
```

The number of neighbors to select for each vector in the HNSW graph.

The default value is 20.

This value controls the tradeoff between search speed and accuracy.
The higher the value the more accurate the search but the slower it will be.

***

### maxIterations?

```ts
optional maxIterations: number;
```

Max iterations to train kmeans.

The default value is 50.

When training an IVF index we use kmeans to calculate the partitions.  This parameter
controls how many iterations of kmeans to run.

Increasing this might improve the quality of the index but in most cases the parameter
is unused because kmeans will converge with fewer iterations.  The parameter is only
used in cases where kmeans does not appear to converge.  In those cases it is unlikely
that setting this larger will lead to the index converging anyways.

***

### numPartitions?

```ts
optional numPartitions: number;
```

The number of IVF partitions to create.

For HNSW, we recommend a small number of partitions. Setting this to 1 works
well for most tables. For very large tables, training just one HNSW graph
will require too much memory. Each partition becomes its own HNSW graph, so
setting this value higher reduces the peak memory use of training.

***

### sampleRate?

```ts
optional sampleRate: number;
```

The rate used to calculate the number of training vectors for kmeans.

Default value is 256.

When an IVF index is trained, we need to calculate partitions.  These are groups
of vectors that are similar to each other.  To do this we use an algorithm called kmeans.

Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
random sample of the data.  This parameter controls the size of the sample.  The total
number of vectors used to train the index is `sample_rate * num_partitions`.

Increasing this value might improve the quality of the index but in most cases the
default should be sufficient.
