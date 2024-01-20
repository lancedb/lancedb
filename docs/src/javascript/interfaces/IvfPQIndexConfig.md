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

[index.ts:942](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L942)

___

### index\_cache\_size

• `Optional` **index\_cache\_size**: `number`

Cache size of the index

#### Defined in

[index.ts:991](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L991)

___

### index\_name

• `Optional` **index\_name**: `string`

A unique name for the index

#### Defined in

[index.ts:947](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L947)

___

### max\_iters

• `Optional` **max\_iters**: `number`

The max number of iterations for kmeans training.

#### Defined in

[index.ts:962](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L962)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Max number of iterations to train OPQ, if `use_opq` is true.

#### Defined in

[index.ts:981](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L981)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, L2 or Cosine

#### Defined in

[index.ts:952](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L952)

___

### num\_bits

• `Optional` **num\_bits**: `number`

The number of bits to present one PQ centroid.

#### Defined in

[index.ts:976](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L976)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

The number of partitions this index

#### Defined in

[index.ts:957](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L957)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Number of subvectors to build PQ code

#### Defined in

[index.ts:972](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L972)

___

### replace

• `Optional` **replace**: `boolean`

Replace an existing index with the same name if it exists.

#### Defined in

[index.ts:986](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L986)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:993](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L993)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Train as optimized product quantization.

#### Defined in

[index.ts:967](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L967)
