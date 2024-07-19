[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Index

# Class: Index

## Methods

### btree()

> `static` **btree**(): [`Index`](Index.md)

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

### ivfPq()

> `static` **ivfPq**(`options`?): [`Index`](Index.md)

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

• **options?**: `Partial`&lt;[`IvfPqOptions`](../interfaces/IvfPqOptions.md)&gt;

#### Returns

[`Index`](Index.md)
