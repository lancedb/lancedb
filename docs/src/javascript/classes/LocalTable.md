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
- [addColumns](LocalTable.md#addcolumns)
- [alterColumns](LocalTable.md#altercolumns)
- [checkElectron](LocalTable.md#checkelectron)
- [cleanupOldVersions](LocalTable.md#cleanupoldversions)
- [compactFiles](LocalTable.md#compactfiles)
- [countRows](LocalTable.md#countrows)
- [createIndex](LocalTable.md#createindex)
- [createScalarIndex](LocalTable.md#createscalarindex)
- [delete](LocalTable.md#delete)
- [dropColumns](LocalTable.md#dropcolumns)
- [filter](LocalTable.md#filter)
- [getSchema](LocalTable.md#getschema)
- [indexStats](LocalTable.md#indexstats)
- [listIndices](LocalTable.md#listindices)
- [mergeInsert](LocalTable.md#mergeinsert)
- [overwrite](LocalTable.md#overwrite)
- [search](LocalTable.md#search)
- [update](LocalTable.md#update)
- [withMiddleware](LocalTable.md#withmiddleware)

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

[index.ts:892](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L892)

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

[index.ts:899](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L899)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:889](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L889)

___

### \_isElectron

• `Private` `Readonly` **\_isElectron**: `boolean`

#### Defined in

[index.ts:888](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L888)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:887](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L887)

___

### \_options

• `Private` `Readonly` **\_options**: () => [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Type declaration

▸ (): [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

##### Returns

[`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Defined in

[index.ts:890](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L890)

___

### \_tbl

• `Private` **\_tbl**: `any`

#### Defined in

[index.ts:886](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L886)

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

[index.ts:938](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L938)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[index.ts:918](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L918)

___

### schema

• `get` **schema**(): `Promise`\<`Schema`\<`any`\>\>

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Implementation of

[Table](../interfaces/Table.md).[schema](../interfaces/Table.md#schema)

#### Defined in

[index.ts:1171](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1171)

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

[index.ts:946](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L946)

___

### addColumns

▸ **addColumns**(`newColumnTransforms`): `Promise`\<`void`\>

Add new columns with defined values.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `newColumnTransforms` | \{ `name`: `string` ; `valueSql`: `string`  }[] | pairs of column names and the SQL expression to use to calculate the value of the new column. These expressions will be evaluated for each row in the table, and can reference existing columns in the table. |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[addColumns](../interfaces/Table.md#addcolumns)

#### Defined in

[index.ts:1195](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1195)

___

### alterColumns

▸ **alterColumns**(`columnAlterations`): `Promise`\<`void`\>

Alter the name or nullability of columns.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `columnAlterations` | [`ColumnAlteration`](../interfaces/ColumnAlteration.md)[] | One or more alterations to apply to columns. |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[alterColumns](../interfaces/Table.md#altercolumns)

#### Defined in

[index.ts:1201](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1201)

___

### checkElectron

▸ `Private` **checkElectron**(): `boolean`

#### Returns

`boolean`

#### Defined in

[index.ts:1183](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1183)

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

[index.ts:1130](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1130)

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

[index.ts:1153](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1153)

___

### countRows

▸ **countRows**(`filter?`): `Promise`\<`number`\>

Returns the number of rows in this table.

#### Parameters

| Name | Type |
| :------ | :------ |
| `filter?` | `string` |

#### Returns

`Promise`\<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[index.ts:1021](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1021)

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

[index.ts:1003](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1003)

___

### createScalarIndex

▸ **createScalarIndex**(`column`, `replace?`): `Promise`\<`void`\>

Create a scalar index on this Table for the given column

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `column` | `string` | The column to index |
| `replace?` | `boolean` | If false, fail if an index already exists on the column it is always set to true for remote connections Scalar indices, like vector indices, can be used to speed up scans. A scalar index can speed up scans that contain filter expressions on the indexed column. For example, the following scan will be faster if the column `my_col` has a scalar index: ```ts const con = await lancedb.connect('./.lancedb'); const table = await con.openTable('images'); const results = await table.where('my_col = 7').execute(); ``` Scalar indices can also speed up scans containing a vector search and a prefilter: ```ts const con = await lancedb.connect('././lancedb'); const table = await con.openTable('images'); const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true); ``` Scalar indices can only speed up scans for basic filters using equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set membership (e.g. `my_col IN (0, 1, 2)`) Scalar indices can be used if the filter contains multiple indexed columns and the filter criteria are AND'd or OR'd together (e.g. `my_col < 0 AND other_col> 100`) Scalar indices may be used if the filter contains non-indexed columns but, depending on the structure of the filter, they may not be usable. For example, if the column `not_indexed` does not have a scalar index then the filter `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on `my_col`. |

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

[index.ts:1011](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1011)

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

[index.ts:1030](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1030)

___

### dropColumns

▸ **dropColumns**(`columnNames`): `Promise`\<`void`\>

Drop one or more columns from the dataset

This is a metadata-only operation and does not remove the data from the
underlying storage. In order to remove the data, you must subsequently
call ``compact_files`` to rewrite the data without the removed columns and
then call ``cleanup_files`` to remove the old files.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `columnNames` | `string`[] | The names of the columns to drop. These can be nested column references (e.g. "a.b.c") or top-level column names (e.g. "a"). |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[dropColumns](../interfaces/Table.md#dropcolumns)

#### Defined in

[index.ts:1205](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1205)

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

#### Implementation of

[Table](../interfaces/Table.md).[filter](../interfaces/Table.md#filter)

#### Defined in

[index.ts:934](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L934)

___

### getSchema

▸ `Private` **getSchema**(): `Promise`\<`Schema`\<`any`\>\>

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Defined in

[index.ts:1176](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1176)

___

### indexStats

▸ **indexStats**(`indexName`): `Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

Get statistics about an index.

#### Parameters

| Name | Type |
| :------ | :------ |
| `indexName` | `string` |

#### Returns

`Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

#### Implementation of

[Table](../interfaces/Table.md).[indexStats](../interfaces/Table.md#indexstats)

#### Defined in

[index.ts:1167](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1167)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

List the indicies on this table.

#### Returns

`Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

#### Implementation of

[Table](../interfaces/Table.md).[listIndices](../interfaces/Table.md#listindices)

#### Defined in

[index.ts:1163](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1163)

___

### mergeInsert

▸ **mergeInsert**(`on`, `data`, `args`): `Promise`\<`void`\>

Runs a "merge insert" operation on the table

This operation can add rows, update rows, and remove rows all in a single
transaction. It is a very generic tool that can be used to create
behaviors like "insert if not exists", "update or insert (i.e. upsert)",
or even replace a portion of existing data with new data (e.g. replace
all data where month="january")

The merge insert operation works by combining new data from a
**source table** with existing data in a **target table** by using a
join.  There are three categories of records.

"Matched" records are records that exist in both the source table and
the target table. "Not matched" records exist only in the source table
(e.g. these are new data) "Not matched by source" records exist only
in the target table (this is old data)

The MergeInsertArgs can be used to customize what should happen for
each category of data.

Please note that the data may appear to be reordered as part of this
operation.  This is because updated rows will be deleted from the
dataset and then reinserted at the end with the new values.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `on` | `string` | a column to join on. This is how records from the source table and target table are matched. |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | the new data to insert |
| `args` | [`MergeInsertArgs`](../interfaces/MergeInsertArgs.md) | parameters controlling how the operation should behave |

#### Returns

`Promise`\<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[mergeInsert](../interfaces/Table.md#mergeinsert)

#### Defined in

[index.ts:1065](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1065)

___

### overwrite

▸ **overwrite**(`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[index.ts:977](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L977)

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

[index.ts:926](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L926)

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

[index.ts:1043](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1043)

___

### withMiddleware

▸ **withMiddleware**(`middleware`): [`Table`](../interfaces/Table.md)\<`T`\>

Instrument the behavior of this Table with middleware.

The middleware will be called in the order they are added.

Currently this functionality is only supported for remote tables.

#### Parameters

| Name | Type |
| :------ | :------ |
| `middleware` | `HttpMiddleware` |

#### Returns

[`Table`](../interfaces/Table.md)\<`T`\>

- this Table instrumented by the passed middleware

#### Implementation of

[Table](../interfaces/Table.md).[withMiddleware](../interfaces/Table.md#withmiddleware)

#### Defined in

[index.ts:1209](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1209)
