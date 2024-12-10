[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Index

# Class: Index

## Methods

### bitmap()

```ts
static bitmap(): Index
```

Create a bitmap index.

A `Bitmap` index stores a bitmap for each distinct value in the column for every row.

This index works best for low-cardinality columns, where the number of unique values
is small (i.e., less than a few hundreds).

#### Returns

[`Index`](Index.md)

***

### btree()

```ts
static btree(): Index
```

Create a btree index

A btree index is an index on a scalar columns.  The index stores a copy of the column
in sorted order.  A header entry is created for each block of rows (currently the
block size is fixed at 4096).  These header entries are stored in a separate
cacheable structure (a btree).  To search for data the header is used to determine
which blocks need to be read from disk.

For example, a btree index in a table with 1Bi rows requires sizeof(Scalar) * 256Ki
bytes of memory and will generally need to read sizeof(Scalar) * 4096 bytes to find
the correct row ids.

This index is good for scalar columns with mostly distinct values and does best when
the query is highly selective.

The btree index does not currently have any parameters though parameters such as the
block size may be added in the future.

#### Returns

[`Index`](Index.md)

***

### fts()

```ts
static fts(options?): Index
```

Create a full text search index

A full text search index is an index on a string column, so that you can conduct full
text searches on the column.

The results of a full text search are ordered by relevance measured by BM25.

You can combine filters with full text search.

For now, the full text search index only supports English, and doesn't support phrase search.

#### Parameters

* **options?**: `Partial`&lt;`FtsOptions`&gt;

#### Returns

[`Index`](Index.md)

***

### hnswPq()

```ts
static hnswPq(options?): Index
```

Create a hnswPq index

HNSW-PQ stands for Hierarchical Navigable Small World - Product Quantization.
It is a variant of the HNSW algorithm that uses product quantization to compress
the vectors.

#### Parameters

* **options?**: `Partial`&lt;`HnswPqOptions`&gt;

#### Returns

[`Index`](Index.md)

***

### hnswSq()

```ts
static hnswSq(options?): Index
```

Create a hnswSq index

HNSW-SQ stands for Hierarchical Navigable Small World - Scalar Quantization.
It is a variant of the HNSW algorithm that uses scalar quantization to compress
the vectors.

#### Parameters

* **options?**: `Partial`&lt;`HnswSqOptions`&gt;

#### Returns

[`Index`](Index.md)

***

### ivfPq()

```ts
static ivfPq(options?): Index
```

Create an IvfPq index

This index stores a compressed (quantized) copy of every vector.  These vectors
are grouped into partitions of similar vectors.  Each partition keeps track of
a centroid which is the average value of all vectors in the group.

During a query the centroids are compared with the query vector to find the closest
partitions.  The compressed vectors in these partitions are then searched to find
the closest vectors.

The compression scheme is called product quantization.  Each vector is divided into
subvectors and then each subvector is quantized into a small number of bits.  the
parameters `num_bits` and `num_subvectors` control this process, providing a tradeoff
between index size (and thus search speed) and index accuracy.

The partitioning process is called IVF and the `num_partitions` parameter controls how
many groups to create.

Note that training an IVF PQ index on a large dataset is a slow operation and
currently is also a memory intensive operation.

#### Parameters

* **options?**: `Partial`&lt;[`IvfPqOptions`](../interfaces/IvfPqOptions.md)&gt;

#### Returns

[`Index`](Index.md)

***

### labelList()

```ts
static labelList(): Index
```

Create a label list index.

LabelList index is a scalar index that can be used on `List<T>` columns to
support queries with `array_contains_all` and `array_contains_any`
using an underlying bitmap index.

#### Returns

[`Index`](Index.md)
