[vectordb](../README.md) / [Exports](../modules.md) / IvfPQIndexConfig

# Interface: IvfPQIndexConfig

## Table of contents

### Properties

- [column](IvfPQIndexConfig.md#column)
- [index\_cache\_size](IvfPQIndexConfig.md#index_cache_size)
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

[index.ts:1282](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1282)

___

### index\_cache\_size

• `Optional` **index\_cache\_size**: `number`

Cache size of the index

#### Defined in

[index.ts:1331](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1331)

___

### index\_name

• `Optional` **index\_name**: `string`

A unique name for the index

#### Defined in

[index.ts:1287](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1287)

___

### max\_iters

• `Optional` **max\_iters**: `number`

The max number of iterations for kmeans training.

#### Defined in

[index.ts:1302](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1302)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Max number of iterations to train OPQ, if `use_opq` is true.

#### Defined in

[index.ts:1321](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1321)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, l2 or Cosine

#### Defined in

[index.ts:1292](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1292)

___

### num\_bits

• `Optional` **num\_bits**: `number`

The number of bits to present one PQ centroid.

#### Defined in

[index.ts:1316](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1316)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

The number of partitions this index

#### Defined in

[index.ts:1297](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1297)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Number of subvectors to build PQ code

#### Defined in

[index.ts:1312](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1312)

___

### replace

• `Optional` **replace**: `boolean`

Replace an existing index with the same name if it exists.

#### Defined in

[index.ts:1326](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1326)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:1333](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1333)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Train as optimized product quantization.

#### Defined in

[index.ts:1307](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1307)
