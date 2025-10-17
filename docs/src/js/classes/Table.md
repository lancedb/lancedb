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

Tables are created using the methods [Connection#createTable](Connection.md#createtable)
and [Connection#createEmptyTable](Connection.md#createemptytable). Existing tables are opened
using [Connection#openTable](Connection.md#opentable).

Closing a table is optional.  It not closed, it will be closed when it is garbage
collected.

## Accessors

### name

```ts
get abstract name(): string
```

Returns the name of the table

#### Returns

`string`

## Methods

### add()

```ts
abstract add(data, options?): Promise<AddResult>
```

Insert records into this Table.

#### Parameters

* **data**: [`Data`](../type-aliases/Data.md)
    Records to be inserted into the Table

* **options?**: `Partial`&lt;[`AddDataOptions`](../interfaces/AddDataOptions.md)&gt;

#### Returns

`Promise`&lt;[`AddResult`](../interfaces/AddResult.md)&gt;

A promise that resolves to an object
containing the new version number of the table

***

### addColumns()

```ts
abstract addColumns(newColumnTransforms): Promise<AddColumnsResult>
```

Add new columns with defined values.

#### Parameters

* **newColumnTransforms**: [`AddColumnsSql`](../interfaces/AddColumnsSql.md)[]
    pairs of column names and
    the SQL expression to use to calculate the value of the new column. These
    expressions will be evaluated for each row in the table, and can
    reference existing columns in the table.

#### Returns

`Promise`&lt;[`AddColumnsResult`](../interfaces/AddColumnsResult.md)&gt;

A promise that resolves to an object
containing the new version number of the table after adding the columns.

***

### alterColumns()

```ts
abstract alterColumns(columnAlterations): Promise<AlterColumnsResult>
```

Alter the name or nullability of columns.

#### Parameters

* **columnAlterations**: [`ColumnAlteration`](../interfaces/ColumnAlteration.md)[]
    One or more alterations to
    apply to columns.

#### Returns

`Promise`&lt;[`AlterColumnsResult`](../interfaces/AlterColumnsResult.md)&gt;

A promise that resolves to an object
containing the new version number of the table after altering the columns.

***

### checkout()

```ts
abstract checkout(version): Promise<void>
```

Checks out a specific version of the table _This is an in-place operation._

This allows viewing previous versions of the table. If you wish to
keep writing to the dataset starting from an old version, then use
the `restore` function.

Calling this method will set the table into time-travel mode. If you
wish to return to standard mode, call `checkoutLatest`.

#### Parameters

* **version**: `string` \| `number`
    The version to checkout, could be version number or tag

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

```ts
abstract checkoutLatest(): Promise<void>
```

Checkout the latest version of the table. _This is an in-place operation._

The table will be set back into standard mode, and will track the latest
version of the table.

#### Returns

`Promise`&lt;`void`&gt;

***

### close()

```ts
abstract close(): void
```

Close the table, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the table after it is closed will result in an error.

#### Returns

`void`

***

### countRows()

```ts
abstract countRows(filter?): Promise<number>
```

Count the total number of rows in the dataset.

#### Parameters

* **filter?**: `string`

#### Returns

`Promise`&lt;`number`&gt;

***

### createIndex()

```ts
abstract createIndex(column, options?): Promise<void>
```

Create an index to speed up queries.

Indices can be created on vector columns or scalar columns.
Indices on vector columns will speed up vector searches.
Indices on scalar columns will speed up filtering (in both
vector and non-vector searches)

We currently don't support custom named indexes.
The index name will always be `${column}_idx`.

#### Parameters

* **column**: `string`

* **options?**: `Partial`&lt;[`IndexOptions`](../interfaces/IndexOptions.md)&gt;

#### Returns

`Promise`&lt;`void`&gt;

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

```ts
abstract delete(predicate): Promise<DeleteResult>
```

Delete the rows that satisfy the predicate.

#### Parameters

* **predicate**: `string`

#### Returns

`Promise`&lt;[`DeleteResult`](../interfaces/DeleteResult.md)&gt;

A promise that resolves to an object
containing the new version number of the table

***

### display()

```ts
abstract display(): string
```

Return a brief description of the table

#### Returns

`string`

***

### dropColumns()

```ts
abstract dropColumns(columnNames): Promise<DropColumnsResult>
```

Drop one or more columns from the dataset

This is a metadata-only operation and does not remove the data from the
underlying storage. In order to remove the data, you must subsequently
call ``compact_files`` to rewrite the data without the removed columns and
then call ``cleanup_files`` to remove the old files.

#### Parameters

* **columnNames**: `string`[]
    The names of the columns to drop. These can
    be nested column references (e.g. "a.b.c") or top-level column names
    (e.g. "a").

#### Returns

`Promise`&lt;[`DropColumnsResult`](../interfaces/DropColumnsResult.md)&gt;

A promise that resolves to an object
containing the new version number of the table after dropping the columns.

***

### dropIndex()

```ts
abstract dropIndex(name): Promise<void>
```

Drop an index from the table.

#### Parameters

* **name**: `string`
    The name of the index.
    This does not delete the index from disk, it just removes it from the table.
    To delete the index, run [Table#optimize](Table.md#optimize) after dropping the index.
    Use [Table.listIndices](Table.md#listindices) to find the names of the indices.

#### Returns

`Promise`&lt;`void`&gt;

***

### indexStats()

```ts
abstract indexStats(name): Promise<undefined | IndexStatistics>
```

List all the stats of a specified index

#### Parameters

* **name**: `string`
    The name of the index.

#### Returns

`Promise`&lt;`undefined` \| [`IndexStatistics`](../interfaces/IndexStatistics.md)&gt;

The stats of the index. If the index does not exist, it will return undefined

Use [Table.listIndices](Table.md#listindices) to find the names of the indices.

***

### isOpen()

```ts
abstract isOpen(): boolean
```

Return true if the table has not been closed

#### Returns

`boolean`

***

### listIndices()

```ts
abstract listIndices(): Promise<IndexConfig[]>
```

List all indices that have been created with [Table.createIndex](Table.md#createindex)

#### Returns

`Promise`&lt;[`IndexConfig`](../interfaces/IndexConfig.md)[]&gt;

***

### listVersions()

```ts
abstract listVersions(): Promise<Version[]>
```

List all the versions of the table

#### Returns

`Promise`&lt;[`Version`](../interfaces/Version.md)[]&gt;

***

### mergeInsert()

```ts
abstract mergeInsert(on): MergeInsertBuilder
```

#### Parameters

* **on**: `string` \| `string`[]

#### Returns

[`MergeInsertBuilder`](MergeInsertBuilder.md)

***

### optimize()

```ts
abstract optimize(options?): Promise<OptimizeStats>
```

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

* **options?**: `Partial`&lt;[`OptimizeOptions`](../interfaces/OptimizeOptions.md)&gt;

#### Returns

`Promise`&lt;[`OptimizeStats`](../interfaces/OptimizeStats.md)&gt;

***

### prewarmIndex()

```ts
abstract prewarmIndex(name): Promise<void>
```

Prewarm an index in the table.

#### Parameters

* **name**: `string`
    The name of the index.
    This will load the index into memory.  This may reduce the cold-start time for
    future queries.  If the index does not fit in the cache then this call may be
    wasteful.

#### Returns

`Promise`&lt;`void`&gt;

***

### query()

```ts
abstract query(): Query
```

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

```ts
abstract restore(): Promise<void>
```

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

```ts
abstract schema(): Promise<Schema<any>>
```

Get the schema of the table.

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;

***

### search()

```ts
abstract search(
   query,
   queryType?,
   ftsColumns?): Query | VectorQuery
```

Create a search query to find the nearest neighbors
of the given query

#### Parameters

* **query**: `string` \| [`IntoVector`](../type-aliases/IntoVector.md) \| [`MultiVector`](../type-aliases/MultiVector.md) \| [`FullTextQuery`](../interfaces/FullTextQuery.md)
    the query, a vector or string

* **queryType?**: `string`
    the type of the query, "vector", "fts", or "auto"

* **ftsColumns?**: `string` \| `string`[]
    the columns to search in for full text search
    for now, only one column can be searched at a time.
    when "auto" is used, if the query is a string and an embedding function is defined, it will be treated as a vector query
    if the query is a string and no embedding function is defined, it will be treated as a full text search query

#### Returns

[`Query`](Query.md) \| [`VectorQuery`](VectorQuery.md)

***

### stats()

```ts
abstract stats(): Promise<TableStatistics>
```

Returns table and fragment statistics

#### Returns

`Promise`&lt;[`TableStatistics`](../interfaces/TableStatistics.md)&gt;

The table and fragment statistics

***

### tags()

```ts
abstract tags(): Promise<Tags>
```

Get a tags manager for this table.

Tags allow you to label specific versions of a table with a human-readable name.
The returned tags manager can be used to list, create, update, or delete tags.

#### Returns

`Promise`&lt;[`Tags`](Tags.md)&gt;

A tags manager for this table

#### Example

```typescript
const tagsManager = await table.tags();
await tagsManager.create("v1", 1);
const tags = await tagsManager.list();
console.log(tags); // { "v1": { version: 1, manifestSize: ... } }
```

***

### takeOffsets()

```ts
abstract takeOffsets(offsets): TakeQuery
```

Create a query that returns a subset of the rows in the table.

#### Parameters

* **offsets**: `number`[]
    The offsets of the rows to return.

#### Returns

[`TakeQuery`](TakeQuery.md)

A builder that can be used to parameterize the query.

***

### takeRowIds()

```ts
abstract takeRowIds(rowIds): TakeQuery
```

Create a query that returns a subset of the rows in the table.

#### Parameters

* **rowIds**: `number`[]
    The row ids of the rows to return.

#### Returns

[`TakeQuery`](TakeQuery.md)

A builder that can be used to parameterize the query.

***

### toArrow()

```ts
abstract toArrow(): Promise<Table<any>>
```

Return the table as an arrow table

#### Returns

`Promise`&lt;`Table`&lt;`any`&gt;&gt;

***

### update()

#### update(opts)

```ts
abstract update(opts): Promise<UpdateResult>
```

Update existing records in the Table

##### Parameters

* **opts**: `object` & `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;

##### Returns

`Promise`&lt;[`UpdateResult`](../interfaces/UpdateResult.md)&gt;

A promise that resolves to an object containing
the number of rows updated and the new version number

##### Example

```ts
table.update({where:"x = 2", values:{"vector": [10, 10]}})
```

#### update(opts)

```ts
abstract update(opts): Promise<UpdateResult>
```

Update existing records in the Table

##### Parameters

* **opts**: `object` & `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;

##### Returns

`Promise`&lt;[`UpdateResult`](../interfaces/UpdateResult.md)&gt;

A promise that resolves to an object containing
the number of rows updated and the new version number

##### Example

```ts
table.update({where:"x = 2", valuesSql:{"x": "x + 1"}})
```

#### update(updates, options)

```ts
abstract update(updates, options?): Promise<UpdateResult>
```

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

* **updates**: `Record`&lt;`string`, `string`&gt; \| `Map`&lt;`string`, `string`&gt;
    the
    columns to update

* **options?**: `Partial`&lt;[`UpdateOptions`](../interfaces/UpdateOptions.md)&gt;
    additional options to control
    the update behavior

##### Returns

`Promise`&lt;[`UpdateResult`](../interfaces/UpdateResult.md)&gt;

A promise that resolves to an object
containing the number of rows updated and the new version number

Keys in the map should specify the name of the column to update.
Values in the map provide the new value of the column.  These can
be SQL literal strings (e.g. "7" or "'foo'") or they can be expressions
based on the row being updated (e.g. "my_col + 1")

***

### updateMetadata()

```ts
abstract updateMetadata(updates, replace?): Promise<Record<string, string>>
```

Update table metadata.

#### Parameters

* **updates**: `Map`&lt;`string`, `null` \| `string`&gt; \| `Record`&lt;`string`, `null` \| `string`&gt;
    The metadata updates to apply. Keys are metadata keys,
    values are the new values. Use `null` to remove a key.

* **replace?**: `boolean`
    If true, replace the entire metadata map. If false, merge
    updates with existing metadata. Defaults to false.

#### Returns

`Promise`&lt;`Record`&lt;`string`, `string`&gt;&gt;

A promise that resolves to the updated metadata map.

#### Example

```ts
// Add metadata
await table.updateMetadata({"description": "My test table", "version": "1.0"});

// Update specific keys
await table.updateMetadata({"version": "1.1", "author": "me"});

// Remove a key
await table.updateMetadata({"author": null});
```

***

### updateSchemaMetadata()

```ts
abstract updateSchemaMetadata(updates, replace?): Promise<Record<string, string>>
```

Update schema metadata.

#### Parameters

* **updates**: `Map`&lt;`string`, `null` \| `string`&gt; \| `Record`&lt;`string`, `null` \| `string`&gt;
    The schema metadata updates to apply. Keys are metadata keys,
    values are the new values. Use `null` to remove a key.

* **replace?**: `boolean`
    If true, replace the entire schema metadata map. If false,
    merge updates with existing metadata. Defaults to false.

#### Returns

`Promise`&lt;`Record`&lt;`string`, `string`&gt;&gt;

A promise that resolves to the updated schema metadata map.

#### Example

```ts
// Add schema metadata
await table.updateSchemaMetadata({"format_version": "2.0"});
```

***

### vectorSearch()

```ts
abstract vectorSearch(vector): VectorQuery
```

Search the table with a given query vector.

This is a convenience method for preparing a vector query and
is the same thing as calling `nearestTo` on the builder returned
by `query`.

#### Parameters

* **vector**: [`IntoVector`](../type-aliases/IntoVector.md) \| [`MultiVector`](../type-aliases/MultiVector.md)

#### Returns

[`VectorQuery`](VectorQuery.md)

#### See

[Query#nearestTo](Query.md#nearestto) for more details.

***

### version()

```ts
abstract version(): Promise<number>
```

Retrieve the version of the table

#### Returns

`Promise`&lt;`number`&gt;

***

### waitForIndex()

```ts
abstract waitForIndex(indexNames, timeoutSeconds): Promise<void>
```

Waits for asynchronous indexing to complete on the table.

#### Parameters

* **indexNames**: `string`[]
    The name of the indices to wait for

* **timeoutSeconds**: `number`
    The number of seconds to wait before timing out
    This will raise an error if the indices are not created and fully indexed within the timeout.

#### Returns

`Promise`&lt;`void`&gt;
