[vectordb](../README.md) / [Exports](../modules.md) / IvfPQIndexConfig

# Interface: IvfPQIndexConfig

## Table of contents

### Properties

- [column](IvfPQIndexConfig.md#column)
- [index\_name](IvfPQIndexConfig.md#index_name)
- [max\_iters](IvfPQIndexConfig.md#max_iters)
- [max\_opq\_iters](IvfPQIndexConfig.md#max_opq_iters)
- [metric\_type](IvfPQIndexConfig.md#metric_type)
- [num\_bits](IvfPQIndexConfig.md#num_bits)
- [num\_partitions](IvfPQIndexConfig.md#num_partitions)
- [num\_sub\_vectors](IvfPQIndexConfig.md#num_sub_vectors)
- [replace](IvfPQIndexConfig.md#replace)
- [type](IvfPQIndexConfig.md#type)
- [use\_opq](IvfPQIndexConfig.md#use_opq)

## Properties

### column

• `Optional` **column**: `string`

The column to be indexed

#### Defined in

[index.ts:382](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L382)

___

### index\_name

• `Optional` **index\_name**: `string`

A unique name for the index

#### Defined in

[index.ts:387](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L387)

___

### max\_iters

• `Optional` **max\_iters**: `number`

The max number of iterations for kmeans training.

#### Defined in

[index.ts:402](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L402)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Max number of iterations to train OPQ, if `use_opq` is true.

#### Defined in

[index.ts:421](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L421)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, L2 or Cosine

#### Defined in

[index.ts:392](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L392)

___

### num\_bits

• `Optional` **num\_bits**: `number`

The number of bits to present one PQ centroid.

#### Defined in

[index.ts:416](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L416)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

The number of partitions this index

#### Defined in

[index.ts:397](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L397)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Number of subvectors to build PQ code

#### Defined in

[index.ts:412](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L412)

___

### replace

• `Optional` **replace**: `boolean`

Replace an existing index with the same name if it exists.

#### Defined in

[index.ts:426](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L426)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:428](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L428)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Train as optimized product quantization.

#### Defined in

[index.ts:407](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L407)
