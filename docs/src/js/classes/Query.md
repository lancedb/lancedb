[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Query

# Class: Query

A builder for LanceDB queries.

## Extends

- [`QueryBase`](QueryBase.md)&lt;`NativeQuery`&gt;

## Constructors

### new Query()

> **new Query**(`tbl`): [`Query`](Query.md)

#### Parameters

• **tbl**: `Table`

#### Returns

[`Query`](Query.md)

#### Overrides

[`QueryBase`](QueryBase.md).[`constructor`](QueryBase.md#constructors)

## Properties

### inner

> `protected` **inner**: `Query` \| `Promise`&lt;`Query`&gt;

#### Inherited from

[`QueryBase`](QueryBase.md).[`inner`](QueryBase.md#inner)

## Methods

### \[asyncIterator\]()

> **\[asyncIterator\]**(): `AsyncIterator`&lt;`RecordBatch`&lt;`any`&gt;, `any`, `undefined`&gt;

#### Returns

`AsyncIterator`&lt;`RecordBatch`&lt;`any`&gt;, `any`, `undefined`&gt;

#### Inherited from

[`QueryBase`](QueryBase.md).[`[asyncIterator]`](QueryBase.md#%5Basynciterator%5D)

***

### doCall()

> `protected` **doCall**(`fn`): `void`

#### Parameters

• **fn**

#### Returns

`void`

#### Inherited from

[`QueryBase`](QueryBase.md).[`doCall`](QueryBase.md#docall)

***

### execute()

> `protected` **execute**(`options`?): [`RecordBatchIterator`](RecordBatchIterator.md)

Execute the query and return the results as an

#### Parameters

• **options?**: `Partial`&lt;`QueryExecutionOptions`&gt;

#### Returns

[`RecordBatchIterator`](RecordBatchIterator.md)

#### See

 - AsyncIterator
of
 - RecordBatch.

By default, LanceDb will use many threads to calculate results and, when
the result set is large, multiple batches will be processed at one time.
This readahead is limited however and backpressure will be applied if this
stream is consumed slowly (this constrains the maximum memory used by a
single query)

#### Inherited from

[`QueryBase`](QueryBase.md).[`execute`](QueryBase.md#execute)

***

### explainPlan()

> **explainPlan**(`verbose`): `Promise`&lt;`string`&gt;

Generates an explanation of the query execution plan.

#### Parameters

• **verbose**: `boolean` = `false`

If true, provides a more detailed explanation. Defaults to false.

#### Returns

`Promise`&lt;`string`&gt;

A Promise that resolves to a string containing the query execution plan explanation.

#### Example

```ts
import * as lancedb from "@lancedb/lancedb"
const db = await lancedb.connect("./.lancedb");
const table = await db.createTable("my_table", [
  { vector: [1.1, 0.9], id: "1" },
]);
const plan = await table.query().nearestTo([0.5, 0.2]).explainPlan();
```

#### Inherited from

[`QueryBase`](QueryBase.md).[`explainPlan`](QueryBase.md#explainplan)

***

### ~~filter()~~

> **filter**(`predicate`): `this`

A filter statement to be applied to this query.

#### Parameters

• **predicate**: `string`

#### Returns

`this`

#### Alias

where

#### Deprecated

Use `where` instead

#### Inherited from

[`QueryBase`](QueryBase.md).[`filter`](QueryBase.md#filter)

***

### limit()

> **limit**(`limit`): `this`

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

• **limit**: `number`

#### Returns

`this`

#### Inherited from

[`QueryBase`](QueryBase.md).[`limit`](QueryBase.md#limit)

***

### nativeExecute()

> `protected` **nativeExecute**(`options`?): `Promise`&lt;`RecordBatchIterator`&gt;

#### Parameters

• **options?**: `Partial`&lt;`QueryExecutionOptions`&gt;

#### Returns

`Promise`&lt;`RecordBatchIterator`&gt;

#### Inherited from

[`QueryBase`](QueryBase.md).[`nativeExecute`](QueryBase.md#nativeexecute)

***

### nearestTo()

> **nearestTo**(`vector`): [`VectorQuery`](VectorQuery.md)

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

• **vector**: `IntoVector`

#### Returns

[`VectorQuery`](VectorQuery.md)

#### See

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

***

### select()

> **select**(`columns`): `this`

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

• **columns**: `string` \| `string`[] \| `Record`&lt;`string`, `string`&gt; \| `Map`&lt;`string`, `string`&gt;

#### Returns

`this`

#### Example

```ts
new Map([["combined", "a + b"], ["c", "c"]])

Columns will always be returned in the order given, even if that order is different than
the order used when adding the data.

Note that you can pass in a `Record<string, string>` (e.g. an object literal). This method
uses `Object.entries` which should preserve the insertion order of the object.  However,
object insertion order is easy to get wrong and `Map` is more foolproof.
```

#### Inherited from

[`QueryBase`](QueryBase.md).[`select`](QueryBase.md#select)

***

### toArray()

> **toArray**(`options`?): `Promise`&lt;`any`[]&gt;

Collect the results as an array of objects.

#### Parameters

• **options?**: `Partial`&lt;`QueryExecutionOptions`&gt;

#### Returns

`Promise`&lt;`any`[]&gt;

#### Inherited from

[`QueryBase`](QueryBase.md).[`toArray`](QueryBase.md#toarray)

***

### toArrow()

> **toArrow**(`options`?): `Promise`&lt;`Table`&lt;`any`&gt;&gt;

Collect the results as an Arrow

#### Parameters

• **options?**: `Partial`&lt;`QueryExecutionOptions`&gt;

#### Returns

`Promise`&lt;`Table`&lt;`any`&gt;&gt;

#### See

ArrowTable.

#### Inherited from

[`QueryBase`](QueryBase.md).[`toArrow`](QueryBase.md#toarrow)

***

### where()

> **where**(`predicate`): `this`

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

• **predicate**: `string`

#### Returns

`this`

#### Example

```ts
x > 10
y > 0 AND y < 100
x > 5 OR y = 'test'

Filtering performance can often be improved by creating a scalar index
on the filter column(s).
```

#### Inherited from

[`QueryBase`](QueryBase.md).[`where`](QueryBase.md#where)
