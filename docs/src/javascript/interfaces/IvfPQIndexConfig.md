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

[index.ts:701](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L701)

___

### index\_cache\_size

• `Optional` **index\_cache\_size**: `number`

Cache size of the index

#### Defined in

[index.ts:750](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L750)

___

### index\_name

• `Optional` **index\_name**: `string`

A unique name for the index

#### Defined in

[index.ts:706](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L706)

___

### max\_iters

• `Optional` **max\_iters**: `number`

The max number of iterations for kmeans training.

#### Defined in

[index.ts:721](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L721)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Max number of iterations to train OPQ, if `use_opq` is true.

#### Defined in

[index.ts:740](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L740)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, L2 or Cosine

#### Defined in

[index.ts:711](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L711)

___

### num\_bits

• `Optional` **num\_bits**: `number`

The number of bits to present one PQ centroid.

#### Defined in

[index.ts:735](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L735)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

The number of partitions this index

#### Defined in

[index.ts:716](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L716)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Number of subvectors to build PQ code

#### Defined in

[index.ts:731](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L731)

___

### replace

• `Optional` **replace**: `boolean`

Replace an existing index with the same name if it exists.

#### Defined in

[index.ts:745](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L745)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:752](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L752)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Train as optimized product quantization.

#### Defined in

[index.ts:726](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L726)
