[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / IvfRqOptions

# Interface: IvfRqOptions

## Properties

### distanceType?

```ts
optional distanceType: "l2" | "cosine" | "dot";
```

Distance type to use to build the index.

Default value is "l2".

This is used when training the index to calculate the IVF partitions
(vectors are grouped in partitions with similar vectors according to this
distance type) and during quantization.

The distance type used to train an index MUST match the distance type used
to search the index. Failure to do so will yield inaccurate results.

The following distance types are available:

"l2" - Euclidean distance.
"cosine" - Cosine distance.
"dot" - Dot product.

***

### maxIterations?

```ts
optional maxIterations: number;
```

Max iterations to train IVF kmeans.

When training an IVF index we use kmeans to calculate the partitions. This parameter
controls how many iterations of kmeans to run.

The default value is 50.

***

### numBits?

```ts
optional numBits: number;
```

Number of bits per dimension for residual quantization.

This value controls how much each residual component is compressed. The more
bits, the more accurate the index will be but the slower search. Typical values
are small integers; the default is 1 bit per dimension.

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
right partition) will be slow. If this value is too small then the second
part of the search (searching within a partition) will be slow.

***

### sampleRate?

```ts
optional sampleRate: number;
```

The number of vectors, per partition, to sample when training IVF kmeans.

When an IVF index is trained, we need to calculate partitions. These are groups
of vectors that are similar to each other. To do this we use an algorithm called kmeans.

Running kmeans on a large dataset can be slow. To speed this up we run kmeans on a
random sample of the data. This parameter controls the size of the sample. The total
number of vectors used to train the index is `sample_rate * num_partitions`.

Increasing this value might improve the quality of the index but in most cases the
default should be sufficient.

The default value is 256.
