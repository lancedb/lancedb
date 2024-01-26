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
- [\_isElectron](LocalTable.md#_iselectron)
- [\_name](LocalTable.md#_name)
- [\_options](LocalTable.md#_options)
- [\_tbl](LocalTable.md#_tbl)
- [where](LocalTable.md#where)

### Accessors

- [name](LocalTable.md#name)
- [schema](LocalTable.md#schema)

### Methods

- [add](LocalTable.md#add)
- [checkElectron](LocalTable.md#checkelectron)
- [cleanupOldVersions](LocalTable.md#cleanupoldversions)
- [compactFiles](LocalTable.md#compactfiles)
- [countRows](LocalTable.md#countrows)
- [createIndex](LocalTable.md#createindex)
- [createScalarIndex](LocalTable.md#createscalarindex)
- [delete](LocalTable.md#delete)
- [filter](LocalTable.md#filter)
- [getSchema](LocalTable.md#getschema)
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

[index.ts:649](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L649)

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

[index.ts:656](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L656)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:646](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L646)

___

### \_isElectron

• `Private` `Readonly` **\_isElectron**: `boolean`

#### Defined in

[index.ts:645](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L645)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:644](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L644)

___

### \_options

• `Private` `Readonly` **\_options**: () => [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Type declaration

▸ (): [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

##### Returns

[`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Defined in

[index.ts:647](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L647)

___

### \_tbl

• `Private` **\_tbl**: `any`

#### Defined in

[index.ts:643](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L643)

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

[index.ts:695](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L695)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[index.ts:675](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L675)

___

### schema

• `get` **schema**(): `Promise`\<`Schema`\<`any`\>\>

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Implementation of

[Table](../interfaces/Table.md).[schema](../interfaces/Table.md#schema)

#### Defined in

[index.ts:875](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L875)

## Methods

### add

▸ **add**(`data`): `Promise`\<`number`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[add](../interfaces/Table.md#add)

#### Defined in

[index.ts:703](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L703)

___

### checkElectron

▸ `Private` **checkElectron**(): `boolean`

#### Returns

`boolean`

#### Defined in

[index.ts:887](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L887)

___

### cleanupOldVersions

▸ **cleanupOldVersions**(`olderThan?`, `deleteUnverified?`): `Promise`\<[`CleanupStats`](../interfaces/CleanupStats.md)\>

Clean up old versions of the table, freeing disk space.

Note: this API is not yet available on LanceDB Cloud

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `olderThan?` | `number` | The minimum age in minutes of the versions to delete. If not provided, defaults to two weeks. |
| `deleteUnverified?` | `boolean` | Because they may be part of an in-progress transaction, uncommitted files newer than 7 days old are not deleted by default. This means that failed transactions can leave around data that takes up disk space for up to 7 days. You can override this safety mechanism by setting this option to `true`, only if you promise there are no in progress writes while you run this operation. Failure to uphold this promise can lead to corrupted tables. |

#### Returns

`Promise`\<[`CleanupStats`](../interfaces/CleanupStats.md)\>

#### Defined in

[index.ts:833](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L833)

___

### compactFiles

▸ **compactFiles**(`options?`): `Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Run the compaction process on the table.

This can be run after making several small appends to optimize the table
for faster reads.

Note: this API is not yet available on LanceDB Cloud

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `options?` | [`CompactionOptions`](../interfaces/CompactionOptions.md) | Advanced options configuring compaction. In most cases, you can omit this arguments, as the default options are sensible for most tables. |

#### Returns

`Promise`\<[`CompactionMetrics`](../interfaces/CompactionMetrics.md)\>

Metrics about the compaction operation.

#### Defined in

[index.ts:857](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L857)

___

### countRows

▸ **countRows**(): `Promise`\<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`\<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[index.ts:773](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L773)

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

[index.ts:758](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L758)

___

### createScalarIndex

▸ **createScalarIndex**(`column`, `replace`): `Promise`\<`void`\>

Create a scalar index on this Table for the given column

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `column` | `string` | The column to index |
| `replace` | `boolean` | If false, fail if an index already exists on the column Scalar indices, like vector indices, can be used to speed up scans. A scalar index can speed up scans that contain filter expressions on the indexed column. For example, the following scan will be faster if the column `my_col` has a scalar index: ```ts const con = await lancedb.connect('./.lancedb'); const table = await con.openTable('images'); const results = await table.where('my_col = 7').execute(); ``` Scalar indices can also speed up scans containing a vector search and a prefilter: ```ts const con = await lancedb.connect('././lancedb'); const table = await con.openTable('images'); const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true); ``` Scalar indices can only speed up scans for basic filters using equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set membership (e.g. `my_col IN (0, 1, 2)`) Scalar indices can be used if the filter contains multiple indexed columns and the filter criteria are AND'd or OR'd together (e.g. `my_col < 0 AND other_col> 100`) Scalar indices may be used if the filter contains non-indexed columns but, depending on the structure of the filter, they may not be usable. For example, if the column `not_indexed` does not have a scalar index then the filter `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on `my_col`. |

#### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect('././lancedb')
const table = await con.openTable('images')
await table.createScalarIndex('my_col')
```

#### Implementation of

[Table](../interfaces/Table.md).[createScalarIndex](../interfaces/Table.md#createscalarindex)

#### Defined in

[index.ts:766](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L766)

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

[index.ts:782](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L782)

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

[index.ts:691](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L691)

___

### getSchema

▸ `Private` **getSchema**(): `Promise`\<`Schema`\<`any`\>\>

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Defined in

[index.ts:880](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L880)

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

[index.ts:871](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L871)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

List the indicies on this table.

#### Returns

`Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

#### Implementation of

[Table](../interfaces/Table.md).[listIndices](../interfaces/Table.md#listindices)

#### Defined in

[index.ts:867](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L867)

___

### overwrite

▸ **overwrite**(`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table Type Table is ArrowTable |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[index.ts:732](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L732)

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

[index.ts:683](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L683)

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

[index.ts:795](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L795)
