[vectordb](../README.md) / [Exports](../modules.md) / LocalTable

# Class: LocalTable\<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implements

- [`Table`](../interfaces/Table.md)\<`T`\>

## Table of contents

### Constructors

- [constructor](LocalTable.md#constructor)

### Properties

- [\_embeddings](LocalTable.md#_embeddings)
- [\_name](LocalTable.md#_name)
- [\_options](LocalTable.md#_options)
- [\_tbl](LocalTable.md#_tbl)
- [where](LocalTable.md#where)

### Accessors

- [name](LocalTable.md#name)

### Methods

- [add](LocalTable.md#add)
- [cleanupOldVersions](LocalTable.md#cleanupoldversions)
- [compactFiles](LocalTable.md#compactfiles)
- [countRows](LocalTable.md#countrows)
- [createIndex](LocalTable.md#createindex)
- [delete](LocalTable.md#delete)
- [filter](LocalTable.md#filter)
- [indexStats](LocalTable.md#indexstats)
- [listIndices](LocalTable.md#listindices)
- [overwrite](LocalTable.md#overwrite)
- [search](LocalTable.md#search)
- [update](LocalTable.md#update)

## Constructors

### constructor

• **new LocalTable**\<`T`\>(`tbl`, `name`, `options`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `tbl` | `any` |
| `name` | `string` |
| `options` | [`ConnectionOptions`](../interfaces/ConnectionOptions.md) |

#### Defined in

[index.ts:464](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L464)

• **new LocalTable**\<`T`\>(`tbl`, `name`, `options`, `embeddings`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `tbl` | `any` |  |
| `name` | `string` |  |
| `options` | [`ConnectionOptions`](../interfaces/ConnectionOptions.md) |  |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> | An embedding function to use when interacting with this table |

#### Defined in

[index.ts:471](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L471)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:461](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L461)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:460](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L460)

___

### \_options

• `Private` `Readonly` **\_options**: () => [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Type declaration

▸ (): [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

##### Returns

[`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Defined in

[index.ts:462](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L462)

___

### \_tbl

• `Private` **\_tbl**: `any`

#### Defined in

[index.ts:459](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L459)

___

### where

• **where**: (`value`: `string`) => [`Query`](Query.md)\<`T`\>

#### Type declaration

▸ (`value`): [`Query`](Query.md)\<`T`\>

Creates a filter query to find all rows matching the specified criteria

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `string` | The filter criteria (like SQL where clause syntax) |

##### Returns

[`Query`](Query.md)\<`T`\>

#### Defined in

[index.ts:499](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L499)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[index.ts:479](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L479)

## Methods

### add

▸ **add**(`data`): `Promise`\<`number`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[add](../interfaces/Table.md#add)

#### Defined in

[index.ts:507](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L507)

___

### cleanupOldVersions

▸ **cleanupOldVersions**(`olderThan?`, `deleteUnverified?`): `Promise`\<[`CleanupStats`](../interfaces/CleanupStats.md)\>

Clean up old versions of the table, freeing disk space.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `olderThan?` | `number` | The minimum age in minutes of the versions to delete. If not provided, defaults to two weeks. |
| `deleteUnverified?` | `boolean` | Because they may be part of an in-progress transaction, uncommitted files newer than 7 days old are not deleted by default. This means that failed transactions can leave around data that takes up disk space for up to 7 days. You can override this safety mechanism by setting this option to `true`, only if you promise there are no in progress writes while you run this operation. Failure to uphold this promise can lead to corrupted tables. |

#### Returns

`Promise`\<[`CleanupStats`](../interfaces/CleanupStats.md)\>

#### Defined in

[index.ts:596](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L596)

___

### compactFiles

▸ **compactFiles**(`options?`): `Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Run the compaction process on the table.

This can be run after making several small appends to optimize the table
for faster reads.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `options?` | [`CompactionOptions`](../interfaces/CompactionOptions.md) | Advanced options configuring compaction. In most cases, you can omit this arguments, as the default options are sensible for most tables. |

#### Returns

`Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Metrics about the compaction operation.

#### Defined in

[index.ts:615](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L615)

___

### countRows

▸ **countRows**(): `Promise`\<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`\<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[index.ts:543](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L543)

___

### createIndex

▸ **createIndex**(`indexParams`): `Promise`\<`any`\>

Create an ANN index on this Table vector index.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | [`IvfPQIndexConfig`](../interfaces/IvfPQIndexConfig.md) | The parameters of this Index, |

#### Returns

`Promise`\<`any`\>

**`See`**

VectorIndexParams.

#### Implementation of

[Table](../interfaces/Table.md).[createIndex](../interfaces/Table.md#createindex)

#### Defined in

[index.ts:536](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L536)

___

### delete

▸ **delete**(`filter`): `Promise`\<`void`\>

Delete rows from this table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[delete](../interfaces/Table.md#delete)

#### Defined in

[index.ts:552](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L552)

___

### filter

▸ **filter**(`value`): [`Query`](Query.md)\<`T`\>

Creates a filter query to find all rows matching the specified criteria

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `value` | `string` | The filter criteria (like SQL where clause syntax) |

#### Returns

[`Query`](Query.md)\<`T`\>

#### Defined in

[index.ts:495](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L495)

___

### indexStats

▸ **indexStats**(`indexUuid`): `Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

Get statistics about an index.

#### Parameters

| Name | Type |
| :------ | :------ |
| `indexUuid` | `string` |

#### Returns

`Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

#### Implementation of

[Table](../interfaces/Table.md).[indexStats](../interfaces/Table.md#indexstats)

#### Defined in

[index.ts:628](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L628)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

List the indicies on this table.

#### Returns

`Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

#### Implementation of

[Table](../interfaces/Table.md).[listIndices](../interfaces/Table.md#listindices)

#### Defined in

[index.ts:624](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L624)

___

### overwrite

▸ **overwrite**(`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[index.ts:522](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L522)

___

### search

▸ **search**(`query`): [`Query`](Query.md)\<`T`\>

Creates a search query to find the nearest neighbors of the given search term

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

#### Returns

[`Query`](Query.md)\<`T`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#search)

#### Defined in

[index.ts:487](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L487)

___

### update

▸ **update**(`args`): `Promise`\<`void`\>

Update rows in this table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `args` | [`UpdateArgs`](../interfaces/UpdateArgs.md) \| [`UpdateSqlArgs`](../interfaces/UpdateSqlArgs.md) | see [UpdateArgs](../interfaces/UpdateArgs.md) and [UpdateSqlArgs](../interfaces/UpdateSqlArgs.md) for more details |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[update](../interfaces/Table.md#update)

#### Defined in

[index.ts:563](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L563)
