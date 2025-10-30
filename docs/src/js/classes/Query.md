[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Query

# Class: Query

A builder for LanceDB queries.

## See

[Table#query](Table.md#query), [Table#search](Table.md#search)

## Extends

- `StandardQueryBase`&lt;`NativeQuery`&gt;

## Properties

### inner

```ts
protected inner: Query | Promise<Query>;
```

#### Inherited from

`StandardQueryBase.inner`

## Methods

### analyzePlan()

```ts
analyzePlan(): Promise<string>
```

Executes the query and returns the physical query plan annotated with runtime metrics.

This is useful for debugging and performance analysis, as it shows how the query was executed
and includes metrics such as elapsed time, rows processed, and I/O statistics.

#### Returns

`Promise`&lt;`string`&gt;

A query execution plan with runtime metrics for each step.

#### Example

```ts
import * as lancedb from "@lancedb/lancedb"

const db = await lancedb.connect("./.lancedb");
const table = await db.createTable("my_table", [
  { vector: [1.1, 0.9], id: "1" },
]);

const plan = await table.query().nearestTo([0.5, 0.2]).analyzePlan();

Example output (with runtime metrics inlined):
AnalyzeExec verbose=true, metrics=[]
 ProjectionExec: expr=[id@3 as id, vector@0 as vector, _distance@2 as _distance], metrics=[output_rows=1, elapsed_compute=3.292µs]
  Take: columns="vector, _rowid, _distance, (id)", metrics=[output_rows=1, elapsed_compute=66.001µs, batches_processed=1, bytes_read=8, iops=1, requests=1]
   CoalesceBatchesExec: target_batch_size=1024, metrics=[output_rows=1, elapsed_compute=3.333µs]
    GlobalLimitExec: skip=0, fetch=10, metrics=[output_rows=1, elapsed_compute=167ns]
     FilterExec: _distance@2 IS NOT NULL, metrics=[output_rows=1, elapsed_compute=8.542µs]
      SortExec: TopK(fetch=10), expr=[_distance@2 ASC NULLS LAST], metrics=[output_rows=1, elapsed_compute=63.25µs, row_replacements=1]
       KNNVectorDistance: metric=l2, metrics=[output_rows=1, elapsed_compute=114.333µs, output_batches=1]
        LanceScan: uri=/path/to/data, projection=[vector], row_id=true, row_addr=false, ordered=false, metrics=[output_rows=1, elapsed_compute=103.626µs, bytes_read=549, iops=2, requests=2]
```

#### Inherited from

`StandardQueryBase.analyzePlan`

***

### execute()

```ts
protected execute(options?): AsyncGenerator<RecordBatch<any>, void, unknown>
```

Execute the query and return the results as an

#### Parameters

* **options?**: `Partial`&lt;[`QueryExecutionOptions`](../interfaces/QueryExecutionOptions.md)&gt;

#### Returns

`AsyncGenerator`&lt;`RecordBatch`&lt;`any`&gt;, `void`, `unknown`&gt;

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

`StandardQueryBase.execute`

***

### explainPlan()

```ts
explainPlan(verbose): Promise<string>
```

Generates an explanation of the query execution plan.

#### Parameters

* **verbose**: `boolean` = `false`
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

`StandardQueryBase.explainPlan`

***

### fastSearch()

```ts
fastSearch(): this
```

Skip searching un-indexed data. This can make search faster, but will miss
any data that is not yet indexed.

Use [Table#optimize](Table.md#optimize) to index all un-indexed data.

#### Returns

`this`

#### Inherited from

`StandardQueryBase.fastSearch`

***

### ~~filter()~~

```ts
filter(predicate): this
```

A filter statement to be applied to this query.

#### Parameters

* **predicate**: `string`

#### Returns

`this`

#### See

where

#### Deprecated

Use `where` instead

#### Inherited from

`StandardQueryBase.filter`

***

### fullTextSearch()

```ts
fullTextSearch(query, options?): this
```

#### Parameters

* **query**: `string` \| [`FullTextQuery`](../interfaces/FullTextQuery.md)

* **options?**: `Partial`&lt;[`FullTextSearchOptions`](../interfaces/FullTextSearchOptions.md)&gt;

#### Returns

`this`

#### Inherited from

`StandardQueryBase.fullTextSearch`

***

### limit()

```ts
limit(limit): this
```

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

* **limit**: `number`

#### Returns

`this`

#### Inherited from

`StandardQueryBase.limit`

***

### nearestTo()

```ts
nearestTo(vector): VectorQuery
```

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

* **vector**: [`IntoVector`](../type-aliases/IntoVector.md)

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

### nearestToText()

```ts
nearestToText(query, columns?): Query
```

#### Parameters

* **query**: `string` \| [`FullTextQuery`](../interfaces/FullTextQuery.md)

* **columns?**: `string`[]

#### Returns

[`Query`](Query.md)

***

### offset()

```ts
offset(offset): this
```

Set the number of rows to skip before returning results.

This is useful for pagination.

#### Parameters

* **offset**: `number`

#### Returns

`this`

#### Inherited from

`StandardQueryBase.offset`

***

### outputSchema()

```ts
outputSchema(): Promise<Schema<any>>
```

Returns the schema of the output that will be returned by this query.

This can be used to inspect the types and names of the columns that will be
returned by the query before executing it.

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;

An Arrow Schema describing the output columns.

#### Inherited from

`StandardQueryBase.outputSchema`

***

### select()

```ts
select(columns): this
```

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

* **columns**: `string` \| `string`[] \| `Record`&lt;`string`, `string`&gt; \| `Map`&lt;`string`, `string`&gt;

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

`StandardQueryBase.select`

***

### toArray()

```ts
toArray(options?): Promise<any[]>
```

Collect the results as an array of objects.

#### Parameters

* **options?**: `Partial`&lt;[`QueryExecutionOptions`](../interfaces/QueryExecutionOptions.md)&gt;

#### Returns

`Promise`&lt;`any`[]&gt;

#### Inherited from

`StandardQueryBase.toArray`

***

### toArrow()

```ts
toArrow(options?): Promise<Table<any>>
```

Collect the results as an Arrow

#### Parameters

* **options?**: `Partial`&lt;[`QueryExecutionOptions`](../interfaces/QueryExecutionOptions.md)&gt;

#### Returns

`Promise`&lt;`Table`&lt;`any`&gt;&gt;

#### See

ArrowTable.

#### Inherited from

`StandardQueryBase.toArrow`

***

### where()

```ts
where(predicate): this
```

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

* **predicate**: `string`

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

`StandardQueryBase.where`

***

### withRowId()

```ts
withRowId(): this
```

Whether to return the row id in the results.

This column can be used to match results between different queries. For
example, to match results from a full text search and a vector search in
order to perform hybrid search.

#### Returns

`this`

#### Inherited from

`StandardQueryBase.withRowId`
