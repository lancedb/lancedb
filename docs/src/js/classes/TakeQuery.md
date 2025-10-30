[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TakeQuery

# Class: TakeQuery

A query that returns a subset of the rows in the table.

## Extends

- [`QueryBase`](QueryBase.md)&lt;`NativeTakeQuery`&gt;

## Properties

### inner

```ts
protected inner: TakeQuery | Promise<TakeQuery>;
```

#### Inherited from

[`QueryBase`](QueryBase.md).[`inner`](QueryBase.md#inner)

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

[`QueryBase`](QueryBase.md).[`analyzePlan`](QueryBase.md#analyzeplan)

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

[`QueryBase`](QueryBase.md).[`execute`](QueryBase.md#execute)

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

[`QueryBase`](QueryBase.md).[`explainPlan`](QueryBase.md#explainplan)

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

[`QueryBase`](QueryBase.md).[`outputSchema`](QueryBase.md#outputschema)

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

[`QueryBase`](QueryBase.md).[`select`](QueryBase.md#select)

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

[`QueryBase`](QueryBase.md).[`toArray`](QueryBase.md#toarray)

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

[`QueryBase`](QueryBase.md).[`toArrow`](QueryBase.md#toarrow)

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

[`QueryBase`](QueryBase.md).[`withRowId`](QueryBase.md#withrowid)
