[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / Query

# Class: Query

A builder for LanceDB queries.

## Hierarchy

- [`QueryBase`](QueryBase.md)\<`NativeQuery`\>

  ↳ **`Query`**

## Table of contents

### Constructors

- [constructor](Query.md#constructor)

### Properties

- [inner](Query.md#inner)

### Methods

- [[asyncIterator]](Query.md#[asynciterator])
- [doCall](Query.md#docall)
- [execute](Query.md#execute)
- [explainPlan](Query.md#explainplan)
- [filter](Query.md#filter)
- [limit](Query.md#limit)
- [nativeExecute](Query.md#nativeexecute)
- [nearestTo](Query.md#nearestto)
- [select](Query.md#select)
- [toArray](Query.md#toarray)
- [toArrow](Query.md#toarrow)
- [where](Query.md#where)

## Constructors

### constructor

• **new Query**(`tbl`): [`Query`](Query.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `tbl` | `Table` |

#### Returns

[`Query`](Query.md)

#### Overrides

[QueryBase](QueryBase.md).[constructor](QueryBase.md#constructor)

#### Defined in

[query.ts:432](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L432)

## Properties

### inner

• `Protected` **inner**: `Query` \| `Promise`\<`Query`\>

#### Inherited from

[QueryBase](QueryBase.md).[inner](QueryBase.md#inner)

#### Defined in

[query.ts:96](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L96)

## Methods

### [asyncIterator]

▸ **[asyncIterator]**(): `AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Returns

`AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Inherited from

[QueryBase](QueryBase.md).[[asyncIterator]](QueryBase.md#[asynciterator])

#### Defined in

[query.ts:226](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L226)

___

### doCall

▸ **doCall**(`fn`): `void`

#### Parameters

| Name | Type |
| :------ | :------ |
| `fn` | (`inner`: `Query`) => `void` |

#### Returns

`void`

#### Inherited from

[QueryBase](QueryBase.md).[doCall](QueryBase.md#docall)

#### Defined in

[query.ts:102](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L102)

___

### execute

▸ **execute**(`options?`): [`RecordBatchIterator`](RecordBatchIterator.md)

Execute the query and return the results as an

#### Parameters

| Name | Type |
| :------ | :------ |
| `options?` | `Partial`\<`QueryExecutionOptions`\> |

#### Returns

[`RecordBatchIterator`](RecordBatchIterator.md)

**`See`**

 - AsyncIterator
of
 - RecordBatch.

By default, LanceDb will use many threads to calculate results and, when
the result set is large, multiple batches will be processed at one time.
This readahead is limited however and backpressure will be applied if this
stream is consumed slowly (this constrains the maximum memory used by a
single query)

#### Inherited from

[QueryBase](QueryBase.md).[execute](QueryBase.md#execute)

#### Defined in

[query.ts:219](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L219)

___

### explainPlan

▸ **explainPlan**(`verbose?`): `Promise`\<`string`\>

Generates an explanation of the query execution plan.

#### Parameters

| Name | Type | Default value | Description |
| :------ | :------ | :------ | :------ |
| `verbose` | `boolean` | `false` | If true, provides a more detailed explanation. Defaults to false. |

#### Returns

`Promise`\<`string`\>

A Promise that resolves to a string containing the query execution plan explanation.

**`Example`**

```ts
import * as lancedb from "@lancedb/lancedb"
const db = await lancedb.connect("./.lancedb");
const table = await db.createTable("my_table", [
  { vector: [1.1, 0.9], id: "1" },
]);
const plan = await table.query().nearestTo([0.5, 0.2]).explainPlan();
```

#### Inherited from

[QueryBase](QueryBase.md).[explainPlan](QueryBase.md#explainplan)

#### Defined in

[query.ts:267](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L267)

___

### filter

▸ **filter**(`predicate`): `this`

A filter statement to be applied to this query.

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

`this`

**`Alias`**

where

**`Deprecated`**

Use `where` instead

#### Inherited from

[QueryBase](QueryBase.md).[filter](QueryBase.md#filter)

#### Defined in

[query.ts:133](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L133)

___

### limit

▸ **limit**(`limit`): `this`

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

| Name | Type |
| :------ | :------ |
| `limit` | `number` |

#### Returns

`this`

#### Inherited from

[QueryBase](QueryBase.md).[limit](QueryBase.md#limit)

#### Defined in

[query.ts:193](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L193)

___

### nativeExecute

▸ **nativeExecute**(`options?`): `Promise`\<`RecordBatchIterator`\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `options?` | `Partial`\<`QueryExecutionOptions`\> |

#### Returns

`Promise`\<`RecordBatchIterator`\>

#### Inherited from

[QueryBase](QueryBase.md).[nativeExecute](QueryBase.md#nativeexecute)

#### Defined in

[query.ts:198](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L198)

___

### nearestTo

▸ **nearestTo**(`vector`): [`VectorQuery`](VectorQuery.md)

Find the nearest vectors to the given query vector.

This converts the query from a plain query to a vector query.

This method will attempt to convert the input to the query vector
expected by the embedding model.  If the input cannot be converted
then an error will be thrown.

By default, there is no embedding model, and the input should be
an array-like object of numbers (something that can be used as input
to Float32Array.from)

If there is only one vector column (a column whose data type is a
fixed size list of floats) then the column does not need to be specified.
If there is more than one vector column you must use

#### Parameters

| Name | Type |
| :------ | :------ |
| `vector` | `IntoVector` |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`See`**

 - [VectorQuery#column](VectorQuery.md#column)  to specify which column you would like
to compare with.

If no index has been created on the vector column then a vector query
will perform a distance comparison between the query vector and every
vector in the database and then sort the results.  This is sometimes
called a "flat search"

For small databases, with a few hundred thousand vectors or less, this can
be reasonably fast.  In larger databases you should create a vector index
on the column.  If there is a vector index then an "approximate" nearest
neighbor search (frequently called an ANN search) will be performed.  This
search is much faster, but the results will be approximate.

The query can be further parameterized using the returned builder.  There
are various ANN search parameters that will let you fine tune your recall
accuracy vs search latency.

Vector searches always have a `limit`.  If `limit` has not been called then
a default `limit` of 10 will be used.
 - [Query#limit](Query.md#limit)

#### Defined in

[query.ts:473](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L473)

___

### select

▸ **select**(`columns`): `this`

Return only the specified columns.

By default a query will return all columns from the table.  However, this can have
a very significant impact on latency.  LanceDb stores data in a columnar fashion.  This
means we can finely tune our I/O to select exactly the columns we need.

As a best practice you should always limit queries to the columns that you need.  If you
pass in an array of column names then only those columns will be returned.

You can also use this method to create new "dynamic" columns based on your existing columns.
For example, you may not care about "a" or "b" but instead simply want "a + b".  This is often
seen in the SELECT clause of an SQL query (e.g. `SELECT a+b FROM my_table`).

To create dynamic columns you can pass in a Map<string, string>.  A column will be returned
for each entry in the map.  The key provides the name of the column.  The value is
an SQL string used to specify how the column is calculated.

For example, an SQL query might state `SELECT a + b AS combined, c`.  The equivalent
input to this method would be:

#### Parameters

| Name | Type |
| :------ | :------ |
| `columns` | `string` \| `string`[] \| `Record`\<`string`, `string`\> \| `Map`\<`string`, `string`\> |

#### Returns

`this`

**`Example`**

```ts
new Map([["combined", "a + b"], ["c", "c"]])

Columns will always be returned in the order given, even if that order is different than
the order used when adding the data.

Note that you can pass in a `Record<string, string>` (e.g. an object literal). This method
uses `Object.entries` which should preserve the insertion order of the object.  However,
object insertion order is easy to get wrong and `Map` is more foolproof.
```

#### Inherited from

[QueryBase](QueryBase.md).[select](QueryBase.md#select)

#### Defined in

[query.ts:167](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L167)

___

### toArray

▸ **toArray**(`options?`): `Promise`\<`any`[]\>

Collect the results as an array of objects.

#### Parameters

| Name | Type |
| :------ | :------ |
| `options?` | `Partial`\<`QueryExecutionOptions`\> |

#### Returns

`Promise`\<`any`[]\>

#### Inherited from

[QueryBase](QueryBase.md).[toArray](QueryBase.md#toarray)

#### Defined in

[query.ts:248](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L248)

___

### toArrow

▸ **toArrow**(`options?`): `Promise`\<`Table`\<`any`\>\>

Collect the results as an Arrow

#### Parameters

| Name | Type |
| :------ | :------ |
| `options?` | `Partial`\<`QueryExecutionOptions`\> |

#### Returns

`Promise`\<`Table`\<`any`\>\>

**`See`**

ArrowTable.

#### Inherited from

[QueryBase](QueryBase.md).[toArrow](QueryBase.md#toarrow)

#### Defined in

[query.ts:232](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L232)

___

### where

▸ **where**(`predicate`): `this`

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

`this`

**`Example`**

```ts
x > 10
y > 0 AND y < 100
x > 5 OR y = 'test'

Filtering performance can often be improved by creating a scalar index
on the filter column(s).
```

#### Inherited from

[QueryBase](QueryBase.md).[where](QueryBase.md#where)

#### Defined in

[query.ts:124](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L124)
