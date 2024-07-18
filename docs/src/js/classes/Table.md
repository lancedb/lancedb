[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / Table

# Class: Table

A Table is a collection of Records in a LanceDB Database.

A Table object is expected to be long lived and reused for multiple operations.
Table objects will cache a certain amount of index data in memory.  This cache
will be freed when the Table is garbage collected.  To eagerly free the cache you
can call the `close` method.  Once the Table is closed, it cannot be used for any
further operations.

Closing a table is optional.  It not closed, it will be closed when it is garbage
collected.

## Table of contents

### Constructors

- [constructor](Table.md#constructor)

### Accessors

- [name](Table.md#name)

### Methods

- [add](Table.md#add)
- [addColumns](Table.md#addcolumns)
- [alterColumns](Table.md#altercolumns)
- [checkout](Table.md#checkout)
- [checkoutLatest](Table.md#checkoutlatest)
- [close](Table.md#close)
- [countRows](Table.md#countrows)
- [createIndex](Table.md#createindex)
- [delete](Table.md#delete)
- [display](Table.md#display)
- [dropColumns](Table.md#dropcolumns)
- [indexStats](Table.md#indexstats)
- [isOpen](Table.md#isopen)
- [listIndices](Table.md#listindices)
- [mergeInsert](Table.md#mergeinsert)
- [optimize](Table.md#optimize)
- [query](Table.md#query)
- [restore](Table.md#restore)
- [schema](Table.md#schema)
- [search](Table.md#search)
- [toArrow](Table.md#toarrow)
- [update](Table.md#update)
- [vectorSearch](Table.md#vectorsearch)
- [version](Table.md#version)
- [parseTableData](Table.md#parsetabledata)

## Constructors

### constructor

• **new Table**(): [`Table`](Table.md)

#### Returns

[`Table`](Table.md)

## Accessors

### name

• `get` **name**(): `string`

Returns the name of the table

#### Returns

`string`

#### Defined in

[table.ts:106](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L106)

## Methods

### add

▸ **add**(`data`, `options?`): `Promise`\<`void`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | [`Data`](../modules.md#data) | Records to be inserted into the Table |
| `options?` | `Partial`\<[`AddDataOptions`](../interfaces/AddDataOptions.md)\> | - |

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:126](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L126)

___

### addColumns

▸ **addColumns**(`newColumnTransforms`): `Promise`\<`void`\>

Add new columns with defined values.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `newColumnTransforms` | [`AddColumnsSql`](../interfaces/AddColumnsSql.md)[] | pairs of column names and the SQL expression to use to calculate the value of the new column. These expressions will be evaluated for each row in the table, and can reference existing columns in the table. |

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:301](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L301)

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

#### Defined in

[table.ts:308](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L308)

___

### checkout

▸ **checkout**(`version`): `Promise`\<`void`\>

Checks out a specific version of the table _This is an in-place operation._

This allows viewing previous versions of the table. If you wish to
keep writing to the dataset starting from an old version, then use
the `restore` function.

Calling this method will set the table into time-travel mode. If you
wish to return to standard mode, call `checkoutLatest`.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `version` | `number` | The version to checkout |

#### Returns

`Promise`\<`void`\>

**`Example`**

```typescript
import * as lancedb from "@lancedb/lancedb"
const db = await lancedb.connect("./.lancedb");
const table = await db.createTable("my_table", [
  { vector: [1.1, 0.9], type: "vector" },
]);

console.log(await table.version()); // 1
console.log(table.display());
await table.add([{ vector: [0.5, 0.2], type: "vector" }]);
await table.checkout(1);
console.log(await table.version()); // 2
```

#### Defined in

[table.ts:349](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L349)

___

### checkoutLatest

▸ **checkoutLatest**(): `Promise`\<`void`\>

Checkout the latest version of the table. _This is an in-place operation._

The table will be set back into standard mode, and will track the latest
version of the table.

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:356](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L356)

___

### close

▸ **close**(): `void`

Close the table, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the table after it is closed will result in an error.

#### Returns

`void`

#### Defined in

[table.ts:117](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L117)

___

### countRows

▸ **countRows**(`filter?`): `Promise`\<`number`\>

Count the total number of rows in the dataset.

#### Parameters

| Name | Type |
| :------ | :------ |
| `filter?` | `string` |

#### Returns

`Promise`\<`number`\>

#### Defined in

[table.ts:186](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L186)

___

### createIndex

▸ **createIndex**(`column`, `options?`): `Promise`\<`void`\>

Create an index to speed up queries.

Indices can be created on vector columns or scalar columns.
Indices on vector columns will speed up vector searches.
Indices on scalar columns will speed up filtering (in both
vector and non-vector searches)

#### Parameters

| Name | Type |
| :------ | :------ |
| `column` | `string` |
| `options?` | `Partial`\<[`IndexOptions`](../interfaces/IndexOptions.md)\> |

#### Returns

`Promise`\<`void`\>

**`Note`**

We currently don't support custom named indexes,
The index name will always be `${column}_idx`

**`Example`**

```ts
// If the column has a vector (fixed size list) data type then
// an IvfPq vector index will be created.
const table = await conn.openTable("my_table");
await table.createIndex("vector");
```

**`Example`**

```ts
// For advanced control over vector index creation you can specify
// the index type and options.
const table = await conn.openTable("my_table");
await table.createIndex("vector", {
  config: lancedb.Index.ivfPq({
    numPartitions: 128,
    numSubVectors: 16,
  }),
});
```

**`Example`**

```ts
// Or create a Scalar index
await table.createIndex("my_float_col");
```

#### Defined in

[table.ts:218](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L218)

___

### delete

▸ **delete**(`predicate`): `Promise`\<`void`\>

Delete the rows that satisfy the predicate.

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:188](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L188)

___

### display

▸ **display**(): `string`

Return a brief description of the table

#### Returns

`string`

#### Defined in

[table.ts:119](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L119)

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

[table.ts:320](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L320)

___

### indexStats

▸ **indexStats**(`name`): `Promise`\<`undefined` \| [`IndexStatistics`](../interfaces/IndexStatistics.md)\>

List all the stats of a specified index

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the index. |

#### Returns

`Promise`\<`undefined` \| [`IndexStatistics`](../interfaces/IndexStatistics.md)\>

The stats of the index. If the index does not exist, it will return undefined

#### Defined in

[table.ts:414](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L414)

___

### isOpen

▸ **isOpen**(): `boolean`

Return true if the table has not been closed

#### Returns

`boolean`

#### Defined in

[table.ts:109](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L109)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`IndexConfig`](../interfaces/IndexConfig.md)[]\>

List all indices that have been created with [Table.createIndex](Table.md#createindex)

#### Returns

`Promise`\<[`IndexConfig`](../interfaces/IndexConfig.md)[]\>

#### Defined in

[table.ts:403](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L403)

___

### mergeInsert

▸ **mergeInsert**(`on`): `MergeInsertBuilder`

#### Parameters

| Name | Type |
| :------ | :------ |
| `on` | `string` \| `string`[] |

#### Returns

`MergeInsertBuilder`

#### Defined in

[table.ts:407](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L407)

___

### optimize

▸ **optimize**(`options?`): `Promise`\<`OptimizeStats`\>

Optimize the on-disk data and indices for better performance.

Modeled after ``VACUUM`` in PostgreSQL.

 Optimization covers three operations:

 - Compaction: Merges small files into larger ones
 - Prune: Removes old versions of the dataset
 - Index: Optimizes the indices, adding new data to existing indices

 Experimental API
 ----------------

 The optimization process is undergoing active development and may change.
 Our goal with these changes is to improve the performance of optimization and
 reduce the complexity.

 That being said, it is essential today to run optimize if you want the best
 performance.  It should be stable and safe to use in production, but it our
 hope that the API may be simplified (or not even need to be called) in the
 future.

 The frequency an application shoudl call optimize is based on the frequency of
 data modifications.  If data is frequently added, deleted, or updated then
 optimize should be run frequently.  A good rule of thumb is to run optimize if
 you have added or modified 100,000 or more records or run more than 20 data
 modification operations.

#### Parameters

| Name | Type |
| :------ | :------ |
| `options?` | `Partial`\<`OptimizeOptions`\> |

#### Returns

`Promise`\<`OptimizeStats`\>

#### Defined in

[table.ts:401](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L401)

___

### query

▸ **query**(): [`Query`](Query.md)

Create a [Query](Query.md) Builder.

Queries allow you to search your existing data.  By default the query will
return all the data in the table in no particular order.  The builder
returned by this method can be used to control the query using filtering,
vector similarity, sorting, and more.

Note: By default, all columns are returned.  For best performance, you should
only fetch the columns you need.

When appropriate, various indices and statistics based pruning will be used to
accelerate the query.

#### Returns

[`Query`](Query.md)

A builder that can be used to parameterize the query

**`Example`**

```ts
// SQL-style filtering
//
// This query will return up to 1000 rows whose value in the `id` column
// is greater than 5. LanceDb supports a broad set of filtering functions.
for await (const batch of table
  .query()
  .where("id > 1")
  .select(["id"])
  .limit(20)) {
  console.log(batch);
}
```

**`Example`**

```ts
// Vector Similarity Search
//
// This example will find the 10 rows whose value in the "vector" column are
// closest to the query vector [1.0, 2.0, 3.0].  If an index has been created
// on the "vector" column then this will perform an ANN search.
//
// The `refineFactor` and `nprobes` methods are used to control the recall /
// latency tradeoff of the search.
for await (const batch of table
  .query()
  .where("id > 1")
  .select(["id"])
  .limit(20)) {
  console.log(batch);
}
```

**`Example`**

```ts
// Scan the full dataset
//
// This query will return everything in the table in no particular order.
for await (const batch of table.query()) {
  console.log(batch);
}
```

#### Defined in

[table.ts:272](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L272)

___

### restore

▸ **restore**(): `Promise`\<`void`\>

Restore the table to the currently checked out version

This operation will fail if checkout has not been called previously

This operation will overwrite the latest version of the table with a
previous version.  Any changes made since the checked out version will
no longer be visible.

Once the operation concludes the table will no longer be in a checked
out state and the read_consistency_interval, if any, will apply.

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:370](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L370)

___

### schema

▸ **schema**(): `Promise`\<`Schema`\<`any`\>\>

Get the schema of the table.

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Defined in

[table.ts:121](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L121)

___

### search

▸ **search**(`query`): [`VectorQuery`](VectorQuery.md)

Create a search query to find the nearest neighbors
of the given query vector

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `string` | the query. This will be converted to a vector using the table's provided embedding function |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`Note`**

If no embedding functions are defined in the table, this will error when collecting the results.

#### Defined in

[table.ts:279](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L279)

▸ **search**(`query`): [`VectorQuery`](VectorQuery.md)

Create a search query to find the nearest neighbors
of the given query vector

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `IntoVector` | the query vector |

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Defined in

[table.ts:285](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L285)

___

### toArrow

▸ **toArrow**(): `Promise`\<`Table`\<`any`\>\>

Return the table as an arrow table

#### Returns

`Promise`\<`Table`\<`any`\>\>

#### Defined in

[table.ts:405](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L405)

___

### update

▸ **update**(`opts`): `Promise`\<`void`\>

Update existing records in the Table

#### Parameters

| Name | Type |
| :------ | :------ |
| `opts` | \{ `values`: `Map`\<`string`, `IntoSql`\> \| `Record`\<`string`, `IntoSql`\>  } & `Partial`\<[`UpdateOptions`](../interfaces/UpdateOptions.md)\> |

#### Returns

`Promise`\<`void`\>

**`Example`**

```ts
table.update({where:"x = 2", values:{"vector": [10, 10]}})
```

#### Defined in

[table.ts:136](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L136)

▸ **update**(`opts`): `Promise`\<`void`\>

Update existing records in the Table

#### Parameters

| Name | Type |
| :------ | :------ |
| `opts` | \{ `valuesSql`: `Record`\<`string`, `string`\> \| `Map`\<`string`, `string`\>  } & `Partial`\<[`UpdateOptions`](../interfaces/UpdateOptions.md)\> |

#### Returns

`Promise`\<`void`\>

**`Example`**

```ts
table.update({where:"x = 2", valuesSql:{"x": "x + 1"}})
```

#### Defined in

[table.ts:150](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L150)

▸ **update**(`updates`, `options?`): `Promise`\<`void`\>

Update existing records in the Table

An update operation can be used to adjust existing values.  Use the
returned builder to specify which columns to update.  The new value
can be a literal value (e.g. replacing nulls with some default value)
or an expression applied to the old value (e.g. incrementing a value)

An optional condition can be specified (e.g. "only update if the old
value is 0")

Note: if your condition is something like "some_id_column == 7" and
you are updating many rows (with different ids) then you will get
better performance with a single [`merge_insert`] call instead of
repeatedly calilng this method.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `updates` | `Record`\<`string`, `string`\> \| `Map`\<`string`, `string`\> | the columns to update Keys in the map should specify the name of the column to update. Values in the map provide the new value of the column. These can be SQL literal strings (e.g. "7" or "'foo'") or they can be expressions based on the row being updated (e.g. "my_col + 1") |
| `options?` | `Partial`\<[`UpdateOptions`](../interfaces/UpdateOptions.md)\> | additional options to control the update behavior |

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:180](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L180)

___

### vectorSearch

▸ **vectorSearch**(`vector`): [`VectorQuery`](VectorQuery.md)

Search the table with a given query vector.

This is a convenience method for preparing a vector query and
is the same thing as calling `nearestTo` on the builder returned
by `query`.

#### Parameters

| Name | Type |
| :------ | :------ |
| `vector` | `IntoVector` |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`See`**

[Query#nearestTo](Query.md#nearestto) for more details.

#### Defined in

[table.ts:293](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L293)

___

### version

▸ **version**(): `Promise`\<`number`\>

Retrieve the version of the table

#### Returns

`Promise`\<`number`\>

#### Defined in

[table.ts:323](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L323)

___

### parseTableData

▸ **parseTableData**(`data`, `options?`, `streaming?`): `Promise`\<\{ `buf`: `Buffer` ; `mode`: `string`  }\>

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `data` | `TableLike` \| `Record`\<`string`, `unknown`\>[] | `undefined` |
| `options?` | `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | `undefined` |
| `streaming` | `boolean` | `false` |

#### Returns

`Promise`\<\{ `buf`: `Buffer` ; `mode`: `string`  }\>

#### Defined in

[table.ts:416](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/table.ts#L416)
