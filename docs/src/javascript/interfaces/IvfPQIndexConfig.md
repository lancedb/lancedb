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

[index.ts:381](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L381)

___

### index\_name

• `Optional` **index\_name**: `string`

A unique name for the index

#### Defined in

[index.ts:386](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L386)

___

### max\_iters

• `Optional` **max\_iters**: `number`

The max number of iterations for kmeans training.

#### Defined in

[index.ts:401](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L401)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Max number of iterations to train OPQ, if `use_opq` is true.

#### Defined in

[index.ts:420](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L420)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, L2 or Cosine

#### Defined in

[index.ts:391](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L391)

___

### num\_bits

• `Optional` **num\_bits**: `number`

The number of bits to present one PQ centroid.

#### Defined in

[index.ts:415](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L415)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

The number of partitions this index

#### Defined in

[index.ts:396](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L396)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Number of subvectors to build PQ code

#### Defined in

[index.ts:411](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L411)

___

### replace

• `Optional` **replace**: `boolean`

Replace an existing index with the same name if it exists.

#### Defined in

[index.ts:425](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L425)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:427](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L427)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Train as optimized product quantization.

#### Defined in

[index.ts:406](https://github.com/lancedb/lancedb/blob/270aedc/node/src/index.ts#L406)
