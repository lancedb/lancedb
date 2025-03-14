[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / IvfFlatOptions

# Interface: IvfFlatOptions

Options to create an `IVF_FLAT` index

## Properties

### distanceType?

```ts
optional distanceType: "l2" | "cosine" | "dot" | "hamming";
```

Distance type to use to build the index.

Default value is "l2".

This is used when training the index to calculate the IVF partitions
(vectors are grouped in partitions with similar vectors according to this
distance type).

The distance type used to train an index MUST match the distance type used
to search the index.  Failure to do so will yield inaccurate results.

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

Note: the cosine distance is undefined when one (or both) of the vectors
are all zeros (there is no direction).  These vectors are invalid and may
never be returned from a vector search.

"dot" - Dot product. Dot distance is the dot product of two vectors. Dot
distance has a range of (-∞, ∞). If the vectors are normalized (i.e. their
l2 norm is 1), then dot distance is equivalent to the cosine distance.

"hamming" - Hamming distance. Hamming distance is a distance metric
calculated from the number of bits that are different between two vectors.
Hamming distance has a range of [0, dimension]. Note that the hamming distance
is only valid for binary vectors.

***

### maxIterations?

```ts
optional maxIterations: number;
```

Max iteration to train IVF kmeans.

When training an IVF_FLAT index we use kmeans to calculate the partitions.  This parameter
controls how many iterations of kmeans to run.

Increasing this might improve the quality of the index but in most cases these extra
iterations have diminishing returns.

The default value is 50.

***

### numPartitions?

```ts
optional numPartitions: number;
```

The number of IVF partitions to create.

This value should generally scale with the number of rows in the dataset.
By default the number of partitions is the square root of the number of
rows.

If this value is too large then the first part of the search (picking the
right partition) will be slow.  If this value is too small then the second
part of the search (searching within a partition) will be slow.

***

### sampleRate?

```ts
optional sampleRate: number;
```

The number of vectors, per partition, to sample when training IVF kmeans.

When an IVF_FLAT index is trained, we need to calculate partitions.  These are groups
of vectors that are similar to each other.  To do this we use an algorithm called kmeans.

Running kmeans on a large dataset can be slow.  To speed this up we run kmeans on a
random sample of the data.  This parameter controls the size of the sample.  The total
number of vectors used to train the index is `sample_rate * num_partitions`.

Increasing this value might improve the quality of the index but in most cases the
default should be sufficient.

The default value is 256.
