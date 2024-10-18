[vectordb](../README.md) / [Exports](../modules.md) / Table

# Interface: Table\<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

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
- [createScalarIndex](Table.md#createscalarindex)
- [delete](Table.md#delete)
- [indexStats](Table.md#indexstats)
- [listIndices](Table.md#listindices)
- [mergeInsert](Table.md#mergeinsert)
- [name](Table.md#name)
- [overwrite](Table.md#overwrite)
- [schema](Table.md#schema)
- [search](Table.md#search)
- [update](Table.md#update)

### Methods

- [addColumns](Table.md#addcolumns)
- [alterColumns](Table.md#altercolumns)
- [dropColumns](Table.md#dropcolumns)
- [filter](Table.md#filter)
- [withMiddleware](Table.md#withmiddleware)

## Properties

### add

• **add**: (`data`: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:381](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L381)

___

### countRows

• **countRows**: (`filter?`: `string`) => `Promise`\<`number`\>

#### Type declaration

▸ (`filter?`): `Promise`\<`number`\>

Returns the number of rows in this table.

##### Parameters

| Name | Type |
| :------ | :------ |
| `filter?` | `string` |

##### Returns

`Promise`\<`number`\>

#### Defined in

[index.ts:454](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L454)

___

### createIndex

• **createIndex**: (`indexParams`: [`IvfPQIndexConfig`](IvfPQIndexConfig.md)) => `Promise`\<`any`\>

#### Type declaration

▸ (`indexParams`): `Promise`\<`any`\>

Create an ANN index on this Table vector index.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | [`IvfPQIndexConfig`](IvfPQIndexConfig.md) | The parameters of this Index, |

##### Returns

`Promise`\<`any`\>

**`See`**

VectorIndexParams.

#### Defined in

[index.ts:398](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L398)

___

### createScalarIndex

• **createScalarIndex**: (`column`: `string`, `replace?`: `boolean`) => `Promise`\<`void`\>

#### Type declaration

▸ (`column`, `replace?`): `Promise`\<`void`\>

Create a scalar index on this Table for the given column

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `column` | `string` | The column to index |
| `replace?` | `boolean` | If false, fail if an index already exists on the column it is always set to true for remote connections Scalar indices, like vector indices, can be used to speed up scans. A scalar index can speed up scans that contain filter expressions on the indexed column. For example, the following scan will be faster if the column `my_col` has a scalar index: ```ts const con = await lancedb.connect('./.lancedb'); const table = await con.openTable('images'); const results = await table.where('my_col = 7').execute(); ``` Scalar indices can also speed up scans containing a vector search and a prefilter: ```ts const con = await lancedb.connect('././lancedb'); const table = await con.openTable('images'); const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true); ``` Scalar indices can only speed up scans for basic filters using equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set membership (e.g. `my_col IN (0, 1, 2)`) Scalar indices can be used if the filter contains multiple indexed columns and the filter criteria are AND'd or OR'd together (e.g. `my_col < 0 AND other_col> 100`) Scalar indices may be used if the filter contains non-indexed columns but, depending on the structure of the filter, they may not be usable. For example, if the column `not_indexed` does not have a scalar index then the filter `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on `my_col`. |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect('././lancedb')
const table = await con.openTable('images')
await table.createScalarIndex('my_col')
```

#### Defined in

[index.ts:449](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L449)

___

### delete

• **delete**: (`filter`: `string`) => `Promise`\<`void`\>

#### Type declaration

▸ (`filter`): `Promise`\<`void`\>

Delete rows from this table.

This can be used to delete a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. The filter must not be empty. |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect("./.lancedb")
const data = [
   {id: 1, vector: [1, 2]},
   {id: 2, vector: [3, 4]},
   {id: 3, vector: [5, 6]},
];
const tbl = await con.createTable("my_table", data)
await tbl.delete("id = 2")
await tbl.countRows() // Returns 2
```

If you have a list of values to delete, you can combine them into a
stringified list and use the `IN` operator:

```ts
const to_remove = [1, 5];
await tbl.delete(`id IN (${to_remove.join(",")})`)
await tbl.countRows() // Returns 1
```

#### Defined in

[index.ts:488](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L488)

___

### indexStats

• **indexStats**: (`indexName`: `string`) => `Promise`\<[`IndexStats`](IndexStats.md)\>

#### Type declaration

▸ (`indexName`): `Promise`\<[`IndexStats`](IndexStats.md)\>

Get statistics about an index.

##### Parameters

| Name | Type |
| :------ | :------ |
| `indexName` | `string` |

##### Returns

`Promise`\<[`IndexStats`](IndexStats.md)\>

#### Defined in

[index.ts:567](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L567)

___

### listIndices

• **listIndices**: () => `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Type declaration

▸ (): `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

List the indicies on this table.

##### Returns

`Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Defined in

[index.ts:562](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L562)

___

### mergeInsert

• **mergeInsert**: (`on`: `string`, `data`: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[], `args`: [`MergeInsertArgs`](MergeInsertArgs.md)) => `Promise`\<`void`\>

#### Type declaration

▸ (`on`, `data`, `args`): `Promise`\<`void`\>

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

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `on` | `string` | a column to join on. This is how records from the source table and target table are matched. |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | the new data to insert |
| `args` | [`MergeInsertArgs`](MergeInsertArgs.md) | parameters controlling how the operation should behave |

##### Returns

`Promise`\<`void`\>

#### Defined in

[index.ts:553](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L553)

___

### name

• **name**: `string`

#### Defined in

[index.ts:367](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L367)

___

### overwrite

• **overwrite**: (`data`: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:389](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L389)

___

### schema

• **schema**: `Promise`\<`Schema`\<`any`\>\>

#### Defined in

[index.ts:571](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L571)

___

### search

• **search**: (`query`: `T`) => [`Query`](../classes/Query.md)\<`T`\>

#### Type declaration

▸ (`query`): [`Query`](../classes/Query.md)\<`T`\>

Creates a search query to find the nearest neighbors of the given search term

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

##### Returns

[`Query`](../classes/Query.md)\<`T`\>

#### Defined in

[index.ts:373](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L373)

___

### update

• **update**: (`args`: [`UpdateArgs`](UpdateArgs.md) \| [`UpdateSqlArgs`](UpdateSqlArgs.md)) => `Promise`\<`void`\>

#### Type declaration

▸ (`args`): `Promise`\<`void`\>

Update rows in this table.

This can be used to update a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `args` | [`UpdateArgs`](UpdateArgs.md) \| [`UpdateSqlArgs`](UpdateSqlArgs.md) | see [UpdateArgs](UpdateArgs.md) and [UpdateSqlArgs](UpdateSqlArgs.md) for more details |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect("./.lancedb")
const data = [
   {id: 1, vector: [3, 3], name: 'Ye'},
   {id: 2, vector: [4, 4], name: 'Mike'},
];
const tbl = await con.createTable("my_table", data)

await tbl.update({
  where: "id = 2",
  values: { vector: [2, 2], name: "Michael" },
})

let results = await tbl.search([1, 1]).execute();
// Returns [
//   {id: 2, vector: [2, 2], name: 'Michael'}
//   {id: 1, vector: [3, 3], name: 'Ye'}
// ]
```

#### Defined in

[index.ts:521](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L521)

## Methods

### addColumns

▸ **addColumns**(`newColumnTransforms`): `Promise`\<`void`\>

Add new columns with defined values.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `newColumnTransforms` | \{ `name`: `string` ; `valueSql`: `string`  }[] | pairs of column names and the SQL expression to use to calculate the value of the new column. These expressions will be evaluated for each row in the table, and can reference existing columns in the table. |

#### Returns

`Promise`\<`void`\>

#### Defined in

[index.ts:582](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L582)

___

### alterColumns

▸ **alterColumns**(`columnAlterations`): `Promise`\<`void`\>

Alter the name or nullability of columns.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `columnAlterations` | [`ColumnAlteration`](ColumnAlteration.md)[] | One or more alterations to apply to columns. |

#### Returns

`Promise`\<`void`\>

#### Defined in

[index.ts:591](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L591)

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

#### Defined in

[index.ts:605](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L605)

___

### filter

▸ **filter**(`value`): [`Query`](../classes/Query.md)\<`T`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `value` | `string` |

#### Returns

[`Query`](../classes/Query.md)\<`T`\>

#### Defined in

[index.ts:569](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L569)

___

### withMiddleware

▸ **withMiddleware**(`middleware`): [`Table`](Table.md)\<`T`\>

Instrument the behavior of this Table with middleware.

The middleware will be called in the order they are added.

Currently this functionality is only supported for remote tables.

#### Parameters

| Name | Type |
| :------ | :------ |
| `middleware` | `HttpMiddleware` |

#### Returns

[`Table`](Table.md)\<`T`\>

- this Table instrumented by the passed middleware

#### Defined in

[index.ts:617](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L617)
