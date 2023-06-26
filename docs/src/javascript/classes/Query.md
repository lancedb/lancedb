[vectordb](../README.md) / [Exports](../modules.md) / Query

# Class: Query<T\>

A builder for nearest neighbor queries for LanceDB.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Table of contents

### Constructors

- [constructor](Query.md#constructor)

### Properties

- [\_embeddings](Query.md#_embeddings)
- [\_filter](Query.md#_filter)
- [\_limit](Query.md#_limit)
- [\_metricType](Query.md#_metrictype)
- [\_nprobes](Query.md#_nprobes)
- [\_query](Query.md#_query)
- [\_queryVector](Query.md#_queryvector)
- [\_refineFactor](Query.md#_refinefactor)
- [\_select](Query.md#_select)
- [\_tbl](Query.md#_tbl)
- [where](Query.md#where)

### Methods

- [execute](Query.md#execute)
- [filter](Query.md#filter)
- [limit](Query.md#limit)
- [metricType](Query.md#metrictype)
- [nprobes](Query.md#nprobes)
- [refineFactor](Query.md#refinefactor)
- [select](Query.md#select)

## Constructors

### constructor

• **new Query**<`T`\>(`tbl`, `query`, `embeddings?`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `tbl` | `any` |
| `query` | `T` |
| `embeddings?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> |

#### Defined in

[index.ts:353](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L353)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\>

#### Defined in

[index.ts:351](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L351)

___

### \_filter

• `Private` `Optional` **\_filter**: `string`

#### Defined in

[index.ts:349](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L349)

___

### \_limit

• `Private` **\_limit**: `number`

#### Defined in

[index.ts:345](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L345)

___

### \_metricType

• `Private` `Optional` **\_metricType**: [`MetricType`](../enums/MetricType.md)

#### Defined in

[index.ts:350](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L350)

___

### \_nprobes

• `Private` **\_nprobes**: `number`

#### Defined in

[index.ts:347](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L347)

___

### \_query

• `Private` `Readonly` **\_query**: `T`

#### Defined in

[index.ts:343](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L343)

___

### \_queryVector

• `Private` `Optional` **\_queryVector**: `number`[]

#### Defined in

[index.ts:344](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L344)

___

### \_refineFactor

• `Private` `Optional` **\_refineFactor**: `number`

#### Defined in

[index.ts:346](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L346)

___

### \_select

• `Private` `Optional` **\_select**: `string`[]

#### Defined in

[index.ts:348](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L348)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:342](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L342)

___

### where

• **where**: (`value`: `string`) => [`Query`](Query.md)<`T`\>

#### Type declaration

▸ (`value`): [`Query`](Query.md)<`T`\>

A filter statement to be applied to this query.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `string` | A filter in the same format used by a sql WHERE clause. |

##### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:401](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L401)

## Methods

### execute

▸ **execute**<`T`\>(): `Promise`<`T`[]\>

Execute the query and return the results as an Array of Objects

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `Record`<`string`, `unknown`\> |

#### Returns

`Promise`<`T`[]\>

#### Defined in

[index.ts:424](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L424)

___

### filter

▸ **filter**(`value`): [`Query`](Query.md)<`T`\>

A filter statement to be applied to this query.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `string` | A filter in the same format used by a sql WHERE clause. |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:396](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L396)

___

### limit

▸ **limit**(`value`): [`Query`](Query.md)<`T`\>

Sets the number of results that will be returned

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `number` | number of results |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:369](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L369)

___

### metricType

▸ **metricType**(`value`): [`Query`](Query.md)<`T`\>

The MetricType used for this Query.

**`See`**

MetricType for the different options

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | [`MetricType`](../enums/MetricType.md) | The metric to the. |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:416](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L416)

___

### nprobes

▸ **nprobes**(`value`): [`Query`](Query.md)<`T`\>

The number of probes used. A higher number makes search more accurate but also slower.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `number` | The number of probes used. |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:387](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L387)

___

### refineFactor

▸ **refineFactor**(`value`): [`Query`](Query.md)<`T`\>

Refine the results by reading extra elements and re-ranking them in memory.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `number` | refine factor to use in this query. |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:378](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L378)

___

### select

▸ **select**(`value`): [`Query`](Query.md)<`T`\>

Return only the specified columns.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `string`[] | Only select the specified columns. If not specified, all columns will be returned. |

#### Returns

[`Query`](Query.md)<`T`\>

#### Defined in

[index.ts:407](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L407)
