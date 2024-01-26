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

[index.ts:968](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L968)

___

### index\_cache\_size

• `Optional` **index\_cache\_size**: `number`

Cache size of the index

#### Defined in

[index.ts:1042](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1042)

___

### index\_name

• `Optional` **index\_name**: `string`

Note: this parameter is not supported on LanceDB Cloud

#### Defined in

[index.ts:976](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L976)

___

### max\_iters

• `Optional` **max\_iters**: `number`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:997](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L997)

___

### max\_opq\_iters

• `Optional` **max\_opq\_iters**: `number`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:1029](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1029)

___

### metric\_type

• `Optional` **metric\_type**: [`MetricType`](../enums/MetricType.md)

Metric type, L2 or Cosine

#### Defined in

[index.ts:981](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L981)

___

### num\_bits

• `Optional` **num\_bits**: `number`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:1021](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1021)

___

### num\_partitions

• `Optional` **num\_partitions**: `number`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:989](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L989)

___

### num\_sub\_vectors

• `Optional` **num\_sub\_vectors**: `number`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:1013](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1013)

___

### replace

• `Optional` **replace**: `boolean`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:1037](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1037)

___

### type

• **type**: ``"ivf_pq"``

#### Defined in

[index.ts:1044](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1044)

___

### use\_opq

• `Optional` **use\_opq**: `boolean`

Note: this parameter is not yet supported on LanceDB Cloud

#### Defined in

[index.ts:1005](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L1005)
