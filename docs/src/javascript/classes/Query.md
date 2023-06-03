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

- [\_columns](Query.md#_columns)
- [\_embeddings](Query.md#_embeddings)
- [\_filter](Query.md#_filter)
- [\_limit](Query.md#_limit)
- [\_metricType](Query.md#_metrictype)
- [\_nprobes](Query.md#_nprobes)
- [\_query](Query.md#_query)
- [\_queryVector](Query.md#_queryvector)
- [\_refineFactor](Query.md#_refinefactor)
- [\_tbl](Query.md#_tbl)

### Methods

- [execute](Query.md#execute)
- [filter](Query.md#filter)
- [limit](Query.md#limit)
- [metricType](Query.md#metrictype)
- [nprobes](Query.md#nprobes)
- [refineFactor](Query.md#refinefactor)

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

[index.ts:241](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L241)

## Properties

### \_columns

• `Private` `Optional` `Readonly` **\_columns**: `string`[]

#### Defined in

[index.ts:236](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L236)

___

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\>

#### Defined in

[index.ts:239](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L239)

___

### \_filter

• `Private` `Optional` **\_filter**: `string`

#### Defined in

[index.ts:237](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L237)

___

### \_limit

• `Private` **\_limit**: `number`

#### Defined in

[index.ts:233](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L233)

___

### \_metricType

• `Private` `Optional` **\_metricType**: [`MetricType`](../enums/MetricType.md)

#### Defined in

[index.ts:238](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L238)

___

### \_nprobes

• `Private` **\_nprobes**: `number`

#### Defined in

[index.ts:235](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L235)

___

### \_query

• `Private` `Readonly` **\_query**: `T`

#### Defined in

[index.ts:231](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L231)

___

### \_queryVector

• `Private` `Optional` **\_queryVector**: `number`[]

#### Defined in

[index.ts:232](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L232)

___

### \_refineFactor

• `Private` `Optional` **\_refineFactor**: `number`

#### Defined in

[index.ts:234](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L234)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:230](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L230)

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

[index.ts:301](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L301)

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

[index.ts:284](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L284)

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

[index.ts:257](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L257)

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

[index.ts:293](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L293)

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

[index.ts:275](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L275)

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

[index.ts:266](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L266)
