[vectordb](../README.md) / [Exports](../modules.md) / Table

# Interface: Table<T\>

A LanceDB table that allows you to search and update a table.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implemented by

- [`LocalTable`](../classes/LocalTable.md)

## Table of contents

### Properties

- [add](Table.md#add)
- [countRows](Table.md#countrows)
- [createIndex](Table.md#createindex)
- [delete](Table.md#delete)
- [name](Table.md#name)
- [overwrite](Table.md#overwrite)
- [search](Table.md#search)

## Properties

### add

• **add**: (`data`: `Record`<`string`, `unknown`\>[]) => `Promise`<`number`\>

#### Type declaration

▸ (`data`): `Promise`<`number`\>

Insert records into this Table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:92](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L92)

___

### countRows

• **countRows**: () => `Promise`<`number`\>

#### Type declaration

▸ (): `Promise`<`number`\>

Returns the number of rows in this table.

##### Returns

`Promise`<`number`\>

#### Defined in

[index.ts:112](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L112)

___

### createIndex

• **createIndex**: (`indexParams`: `IvfPQIndexConfig`) => `Promise`<`any`\>

#### Type declaration

▸ (`indexParams`): `Promise`<`any`\>

Create an ANN index on this Table vector index.

**`See`**

VectorIndexParams.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | `IvfPQIndexConfig` | The parameters of this Index, |

##### Returns

`Promise`<`any`\>

#### Defined in

[index.ts:107](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L107)

___

### delete

• **delete**: (`filter`: `string`) => `Promise`<`void`\>

#### Type declaration

▸ (`filter`): `Promise`<`void`\>

Delete rows from this table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. |

##### Returns

`Promise`<`void`\>

#### Defined in

[index.ts:119](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L119)

___

### name

• **name**: `string`

#### Defined in

[index.ts:78](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L78)

___

### overwrite

• **overwrite**: (`data`: `Record`<`string`, `unknown`\>[]) => `Promise`<`number`\>

#### Type declaration

▸ (`data`): `Promise`<`number`\>

Insert records into this Table, replacing its contents.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:100](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L100)

___

### search

• **search**: (`query`: `T`) => [`Query`](../classes/Query.md)<`T`\>

#### Type declaration

▸ (`query`): [`Query`](../classes/Query.md)<`T`\>

Creates a search query to find the nearest neighbors of the given search term

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

##### Returns

[`Query`](../classes/Query.md)<`T`\>

#### Defined in

[index.ts:84](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L84)
