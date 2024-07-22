[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Table

# Class: `abstract` Table

A Table is a collection of Records in a LanceDB Database.

A Table object is expected to be long lived and reused for multiple operations.
Table objects will cache a certain amount of index data in memory.  This cache
will be freed when the Table is garbage collected.  To eagerly free the cache you
can call the `close` method.  Once the Table is closed, it cannot be used for any
further operations.

Closing a table is optional.  It not closed, it will be closed when it is garbage
collected.

## Constructors

### new Table()

> **new Table**(): [`Table`](Table.md)

#### Returns

[`Table`](Table.md)

## Accessors

### name

> `get` `abstract` **name**(): `string`

Returns the name of the table

#### Returns

`string`

## Methods

### add()

> `abstract` **add**(`data`, `options`?): `Promise`&lt;`void`&gt;

Insert records into this Table.

#### Parameters

• **data**: [`Data`](../type-aliases/Data.md)

Records to be inserted into the Table

• **options?**: `Partial`&lt;[`AddDataOptions`](../interfaces/AddDataOptions.md)&gt;

#### Returns

`Promise`&lt;`void`&gt;

***

### addColumns()

> `abstract` **addColumns**(`newColumnTransforms`): `Promise`&lt;`void`&gt;

Add new columns with defined values.

#### Parameters

• **newColumnTransforms**: [`AddColumnsSql`](../interfaces/AddColumnsSql.md)[]

pairs of column names and
the SQL expression to use to calculate the value of the new column. These
expressions will be evaluated for each row in the table, and can
reference existing columns in the table.

#### Returns

`Promise`&lt;`void`&gt;

***

### alterColumns()

> `abstract` **alterColumns**(`columnAlterations`): `Promise`&lt;`void`&gt;

Alter the name or nullability of columns.

#### Parameters

• **columnAlterations**: [`ColumnAlteration`](../interfaces/ColumnAlteration.md)[]

One or more alterations to
apply to columns.

#### Returns

`Promise`&lt;`void`&gt;

***

### checkout()

> `abstract` **checkout**(`version`): `Promise`&lt;`void`&gt;

Checks out a specific version of the table _This is an in-place operation._

This allows viewing previous versions of the table. If you wish to
keep writing to the dataset starting from an old version, then use
the `restore` function.

Calling this method will set the table into time-travel mode. If you
wish to return to standard mode, call `checkoutLatest`.

#### Parameters

• **version**: `number`

The version to checkout

#### Returns

`Promise`&lt;`void`&gt;

#### Example

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

***

### checkoutLatest()

> `abstract` **checkoutLatest**(): `Promise`&lt;`void`&gt;

Checkout the latest version of the table. _This is an in-place operation._

The table will be set back into standard mode, and will track the latest
version of the table.

#### Returns

`Promise`&lt;`void`&gt;

***

### close()

> `abstract` **close**(): `void`

Close the table, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the table after it is closed will result in an error.

#### Returns

`void`

***

### countRows()

> `abstract` **countRows**(`filter`?): `Promise`&lt;`number`&gt;

Count the total number of rows in the dataset.

#### Parameters

• **filter?**: `string`

#### Returns

`Promise`&lt;`number`&gt;

***

### createIndex()

> `abstract` **createIndex**(`column`, `options`?): `Promise`&lt;`void`&gt;

Create an index to speed up queries.

Indices can be created on vector columns or scalar columns.
Indices on vector columns will speed up vector searches.
Indices on scalar columns will speed up filtering (in both
vector and non-vector searches)

#### Parameters

• **column**: `string`

• **options?**: `Partial`&lt;[`IndexOptions`](../interfaces/IndexOptions.md)&gt;

#### Returns

`Promise`&lt;`void`&gt;

#### Note

We currently don't support custom named indexes,
The index name will always be `${column}_idx`

#### Examples

```ts
// If the column has a vector (fixed size list) data type then
// an IvfPq vector index will be created.
const table = await conn.openTable("my_table");
await table.createIndex("vector");
```

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

```ts
// Or create a Scalar index
await table.createIndex("my_float_col");
```

***

### delete()

> `abstract` **delete**(`predicate`): `Promise`&lt;`void`&gt;

Delete the rows that satisfy the predicate.

#### Parameters

• **predicate**: `string`

#### Returns

`Promise`&lt;`void`&gt;

***

### display()

> `abstract` **display**(): `string`

Return a brief description of the table

#### Returns

`string`

***

### dropColumns()

> `abstract` **dropColumns**(`columnNames`): `Promise`&lt;`void`&gt;

Drop one or more columns from the dataset

This is a metadata-only operation and does not remove the data from the
underlying storage. In order to remove the data, you must subsequently
call ``compact_files`` to rewrite the data without the removed columns and
then call ``cleanup_files`` to remove the old files.

#### Parameters

• **columnNames**: `string`[]

The names of the columns to drop. These can
be nested column references (e.g. "a.b.c") or top-level column names
(e.g. "a").

#### Returns

`Promise`&lt;`void`&gt;

***

### indexStats()

> `abstract` **indexStats**(`name`): `Promise`&lt;`undefined` \| [`IndexStatistics`](../interfaces/IndexStatistics.md)&gt;

List all the stats of a specified index

#### Parameters

• **name**: `string`

The name of the index.

#### Returns

`Promise`&lt;`undefined` \| [`IndexStatistics`](../interfaces/IndexStatistics.md)&gt;

The stats of the index. If the index does not exist, it will return undefined

***

### isOpen()

> `abstract` **isOpen**(): `boolean`

Return true if the table has not been closed

#### Returns

`boolean`

***

### listIndices()

> `abstract` **listIndices**(): `Promise`&lt;[`IndexConfig`](../interfaces/IndexConfig.md)[]&gt;

List all indices that have been created with [Table.createIndex](Table.md#createindex)

#### Returns

`Promise`&lt;[`IndexConfig`](../interfaces/IndexConfig.md)[]&gt;

***

### mergeInsert()

> `abstract` **mergeInsert**(`on`): `MergeInsertBuilder`

#### Parameters

• **on**: `string` \| `string`[]

#### Returns

`MergeInsertBuilder`

***

### optimize()

> `abstract` **optimize**(`options`?): `Promise`&lt;`OptimizeStats`&gt;

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

• **options?**: `Partial`&lt;`OptimizeOptions`&gt;

#### Returns

`Promise`&lt;`OptimizeStats`&gt;

***

### query()

> `abstract` **query**(): [`Query`](Query.md)

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

#### Examples

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

```ts
// Scan the full dataset
//
// This query will return everything in the table in no particular order.
for await (const batch of table.query()) {
  console.log(batch);
}
```

***

### restore()

> `abstract` **restore**(): `Promise`&lt;`void`&gt;

Restore the table to the currently checked out version

This operation will fail if checkout has not been called previously

This operation will overwrite the latest version of the table with a
previous version.  Any changes made since the checked out version will
no longer be visible.

Once the operation concludes the table will no longer be in a checked
out state and the read_consistency_interval, if any, will apply.

#### Returns

`Promise`&lt;`void`&gt;

***

### schema()

> `abstract` **schema**(): `Promise`&lt;`Schema`&lt;`any`&gt;&gt;

Get the schema of the table.

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;

***

### search()

#### search(query)

> `abstract` **search**(`query`): [`VectorQuery`](VectorQuery.md)

Create a search query to find the nearest neighbors
of the given query vector

##### Parameters

• **query**: `string`

the query. This will be converted to a vector using the table's provided embedding function

##### Returns

[`VectorQuery`](VectorQuery.md)

##### Note

If no embedding functions are defined in the table, this will error when collecting the results.

#### search(query)

> `abstract` **search**(`query`): [`VectorQuery`](VectorQuery.md)

Create a search query to find the nearest neighbors
of the given query vector

##### Parameters

• **query**: `IntoVector`

the query vector

##### Returns

[`VectorQuery`](VectorQuery.md)

***

### toArrow()

> `abstract` **toArrow**(): `Promise`&lt;`Table`&lt;`any`&gt;&gt;

Return the table as an arrow table

#### Returns

`Promise`&lt;`Table`&lt;`any`&gt;&gt;

***

### update()

#### update(opts)

> `abstract` **update**(`opts`): `Promise`&lt;`void`&gt;

Update existing records in the Table

##### Parameters

• **opts**: `object` & `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;

##### Returns

`Promise`&lt;`void`&gt;

##### Example

```ts
table.update({where:"x = 2", values:{"vector": [10, 10]}})
```

#### update(opts)

> `abstract` **update**(`opts`): `Promise`&lt;`void`&gt;

Update existing records in the Table

##### Parameters

• **opts**: `object` & `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;

##### Returns

`Promise`&lt;`void`&gt;

##### Example

```ts
table.update({where:"x = 2", valuesSql:{"x": "x + 1"}})
```

#### update(updates, options)

> `abstract` **update**(`updates`, `options`?): `Promise`&lt;`void`&gt;

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

##### Parameters

• **updates**: `Record`&lt;`string`, `string`&gt; \| `Map`&lt;`string`, `string`&gt;

the
columns to update

Keys in the map should specify the name of the column to update.
Values in the map provide the new value of the column.  These can
be SQL literal strings (e.g. "7" or "'foo'") or they can be expressions
based on the row being updated (e.g. "my_col + 1")

• **options?**: `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;

additional options to control
the update behavior

##### Returns

`Promise`&lt;`void`&gt;

***

### vectorSearch()

> `abstract` **vectorSearch**(`vector`): [`VectorQuery`](VectorQuery.md)

Search the table with a given query vector.

This is a convenience method for preparing a vector query and
is the same thing as calling `nearestTo` on the builder returned
by `query`.

#### Parameters

• **vector**: `IntoVector`

#### Returns

[`VectorQuery`](VectorQuery.md)

#### See

[Query#nearestTo](Query.md#nearestto) for more details.

***

### version()

> `abstract` **version**(): `Promise`&lt;`number`&gt;

Retrieve the version of the table

#### Returns

`Promise`&lt;`number`&gt;

***

### parseTableData()

> `static` **parseTableData**(`data`, `options`?, `streaming`?): `Promise`&lt;`object`&gt;

#### Parameters

• **data**: `TableLike` \| `Record`&lt;`string`, `unknown`&gt;[]

• **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;

• **streaming?**: `boolean` = `false`

#### Returns

`Promise`&lt;`object`&gt;

##### buf

> **buf**: `Buffer`

##### mode

> **mode**: `string`
