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

### Properties

- [inner](Table.md#inner)

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
- [isOpen](Table.md#isopen)
- [listIndices](Table.md#listindices)
- [query](Table.md#query)
- [restore](Table.md#restore)
- [schema](Table.md#schema)
- [update](Table.md#update)
- [vectorSearch](Table.md#vectorsearch)
- [version](Table.md#version)

## Constructors

### constructor

• **new Table**(`inner`): [`Table`](Table.md)

Construct a Table. Internal use only.

#### Parameters

| Name | Type |
| :------ | :------ |
| `inner` | `Table` |

#### Returns

[`Table`](Table.md)

#### Defined in

[table.ts:69](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L69)

## Properties

### inner

• `Private` `Readonly` **inner**: `Table`

#### Defined in

[table.ts:66](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L66)

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

[table.ts:105](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L105)

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

[table.ts:261](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L261)

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

[table.ts:270](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L270)

___

### checkout

▸ **checkout**(`version`): `Promise`\<`void`\>

Checks out a specific version of the Table

Any read operation on the table will now access the data at the checked out version.
As a consequence, calling this method will disable any read consistency interval
that was previously set.

This is a read-only operation that turns the table into a sort of "view"
or "detached head".  Other table instances will not be affected.  To make the change
permanent you can use the `[Self::restore]` method.

Any operation that modifies the table will fail while the table is in a checked
out state.

To return the table to a normal state use `[Self::checkout_latest]`

#### Parameters

| Name | Type |
| :------ | :------ |
| `version` | `number` |

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:317](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L317)

___

### checkoutLatest

▸ **checkoutLatest**(): `Promise`\<`void`\>

Ensures the table is pointing at the latest version

This can be used to manually update a table when the read_consistency_interval is None
It can also be used to undo a `[Self::checkout]` operation

#### Returns

`Promise`\<`void`\>

#### Defined in

[table.ts:327](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L327)

___

### close

▸ **close**(): `void`

Close the table, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the table after it is closed will result in an error.

#### Returns

`void`

#### Defined in

[table.ts:85](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L85)

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

[table.ts:152](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L152)

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

**`Example`**

```ts
// If the column has a vector (fixed size list) data type then
// an IvfPq vector index will be created.
const table = await conn.openTable("my_table");
await table.createIndex(["vector"]);
```

**`Example`**

```ts
// For advanced control over vector index creation you can specify
// the index type and options.
const table = await conn.openTable("my_table");
await table.createIndex(["vector"], I)
  .ivf_pq({ num_partitions: 128, num_sub_vectors: 16 })
  .build();
```

**`Example`**

```ts
// Or create a Scalar index
await table.createIndex("my_float_col").build();
```

#### Defined in

[table.ts:184](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L184)

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

[table.ts:157](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L157)

___

### display

▸ **display**(): `string`

Return a brief description of the table

#### Returns

`string`

#### Defined in

[table.ts:90](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L90)

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

[table.ts:285](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L285)

___

### isOpen

▸ **isOpen**(): `boolean`

Return true if the table has not been closed

#### Returns

`boolean`

#### Defined in

[table.ts:74](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L74)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`IndexConfig`](../interfaces/IndexConfig.md)[]\>

List all indices that have been created with Self::create_index

#### Returns

`Promise`\<[`IndexConfig`](../interfaces/IndexConfig.md)[]\>

#### Defined in

[table.ts:350](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L350)

___

### query

▸ **query**(): [`Query`](Query.md)

Create a [Query](Query.md) Builder.

Queries allow you to search your existing data.  By default the query will
return all the data in the table in no particular order.  The builder
returned by this method can be used to control the query using filtering,
vector similarity, sorting, and more.

Note: By default, all columns are returned.  For best performance, you should
only fetch the columns you need.  See [`Query::select_with_projection`] for
more details.

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
// is greater than 5.  LanceDb supports a broad set of filtering functions.
for await (const batch of table.query()
                         .filter("id > 1").select(["id"]).limit(20)) {
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
// The `refine_factor` and `nprobes` methods are used to control the recall /
// latency tradeoff of the search.
for await (const batch of table.query()
                   .nearestTo([1, 2, 3])
                   .refineFactor(5).nprobe(10)
                   .limit(10)) {
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

[table.ts:238](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L238)

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

[table.ts:343](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L343)

___

### schema

▸ **schema**(): `Promise`\<`Schema`\<`any`\>\>

Get the schema of the table.

#### Returns

`Promise`\<`Schema`\<`any`\>\>

#### Defined in

[table.ts:95](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L95)

___

### update

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

[table.ts:137](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L137)

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
| `vector` | `unknown` |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`See`**

[Query#nearestTo](Query.md#nearestto) for more details.

#### Defined in

[table.ts:249](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L249)

___

### version

▸ **version**(): `Promise`\<`number`\>

Retrieve the version of the table

LanceDb supports versioning.  Every operation that modifies the table increases
version.  As long as a version hasn't been deleted you can `[Self::checkout]` that
version to view the data at that point.  In addition, you can `[Self::restore]` the
version to replace the current table with a previous version.

#### Returns

`Promise`\<`number`\>

#### Defined in

[table.ts:297](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L297)
