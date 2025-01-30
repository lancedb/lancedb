[**@lancedb/lancedb**](../README.md) • **Docs**
***
[@lancedb/lancedb](../globals.md) / VectorQuery
# Class: VectorQuery
A builder used to construct a vector search
This builder can be reused to execute the query many times.
## See
[Query#nearestTo](Query.md#nearestto)
## Extends
- [`QueryBase`](QueryBase.md)&lt;`NativeVectorQuery`&gt;
## Properties
### inner
```ts
protected inner: VectorQuery | Promise<VectorQuery>;
```
#### Inherited from
[`QueryBase`](QueryBase.md).[`inner`](QueryBase.md#inner)
## Methods
### addQueryVector()
```ts
addQueryVector(vector): VectorQuery
```
#### Parameters
* **vector**: [`IntoVector`](../type-aliases/IntoVector.md)
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### bypassVectorIndex()
```ts
bypassVectorIndex(): VectorQuery
```
If this is called then any vector index is skipped
An exhaustive (flat) search will be performed.  The query vector will
be compared to every vector in the table.  At high scales this can be
expensive.  However, this is often still useful.  For example, skipping
the vector index can give you ground truth results which you can use to
calculate your recall to select an appropriate value for nprobes.
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### column()
```ts
column(column): VectorQuery
```
Set the vector column to query
This controls which column is compared to the query vector supplied in
the call to
#### Parameters
* **column**: `string`
#### Returns
[`VectorQuery`](VectorQuery.md)
#### See
[Query#nearestTo](Query.md#nearestto)
This parameter must be specified if the table has more than one column
whose data type is a fixed-size-list of floats.
***
### distanceRange()
```ts
distanceRange(lowerBound?, upperBound?): VectorQuery
```
#### Parameters
* **lowerBound?**: `number`
* **upperBound?**: `number`
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### distanceType()
```ts
distanceType(distanceType): VectorQuery
```
Set the distance metric to use
When performing a vector search we try and find the "nearest" vectors according
to some kind of distance metric.  This parameter controls which distance metric to
use.  See
#### Parameters
* **distanceType**: `"l2"` \| `"cosine"` \| `"dot"`
#### Returns
[`VectorQuery`](VectorQuery.md)
#### See
[IvfPqOptions.distanceType](../interfaces/IvfPqOptions.md#distancetype) for more details on the different
distance metrics available.
Note: if there is a vector index then the distance type used MUST match the distance
type used to train the vector index.  If this is not done then the results will be
invalid.
By default "l2" is used.
***
### ef()
```ts
ef(ef): VectorQuery
```
Set the number of candidates to consider during the search
This argument is only used when the vector column has an HNSW index.
If there is no index then this value is ignored.
Increasing this value will increase the recall of your query but will
also increase the latency of your query. The default value is 1.5*limit.
#### Parameters
* **ef**: `number`
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### execute()
```ts
protected execute(options?): RecordBatchIterator
```
Execute the query and return the results as an
#### Parameters
* **options?**: `Partial`&lt;[`QueryExecutionOptions`](../interfaces/QueryExecutionOptions.md)&gt;
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
[`QueryBase`](QueryBase.md).[`fastSearch`](QueryBase.md#fastsearch)
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
[`QueryBase`](QueryBase.md).[`filter`](QueryBase.md#filter)
***
### fullTextSearch()
```ts
fullTextSearch(query, options?): this
```
#### Parameters
* **query**: `string`
* **options?**: `Partial`&lt;[`FullTextSearchOptions`](../interfaces/FullTextSearchOptions.md)&gt;
#### Returns
`this`
#### Inherited from
[`QueryBase`](QueryBase.md).[`fullTextSearch`](QueryBase.md#fulltextsearch)
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
[`QueryBase`](QueryBase.md).[`limit`](QueryBase.md#limit)
***
### nprobes()
```ts
nprobes(nprobes): VectorQuery
```
Set the number of partitions to search (probe)
This argument is only used when the vector column has an IVF PQ index.
If there is no index then this value is ignored.
The IVF stage of IVF PQ divides the input into partitions (clusters) of
related values.
The partition whose centroids are closest to the query vector will be
exhaustiely searched to find matches.  This parameter controls how many
partitions should be searched.
Increasing this value will increase the recall of your query but will
also increase the latency of your query.  The default value is 20.  This
default is good for many cases but the best value to use will depend on
your data and the recall that you need to achieve.
For best results we recommend tuning this parameter with a benchmark against
your actual data to find the smallest possible value that will still give
you the desired recall.
#### Parameters
* **nprobes**: `number`
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### offset()
```ts
offset(offset): this
```
#### Parameters
* **offset**: `number`
#### Returns
`this`
#### Inherited from
[`QueryBase`](QueryBase.md).[`offset`](QueryBase.md#offset)
***
### postfilter()
```ts
postfilter(): VectorQuery
```
If this is called then filtering will happen after the vector search instead of
before.
By default filtering will be performed before the vector search.  This is how
filtering is typically understood to work.  This prefilter step does add some
additional latency.  Creating a scalar index on the filter column(s) can
often improve this latency.  However, sometimes a filter is too complex or scalar
indices cannot be applied to the column.  In these cases postfiltering can be
used instead of prefiltering to improve latency.
Post filtering applies the filter to the results of the vector search.  This means
we only run the filter on a much smaller set of data.  However, it can cause the
query to return fewer than `limit` results (or even no results) if none of the nearest
results match the filter.
Post filtering happens during the "refine stage" (described in more detail in
#### Returns
[`VectorQuery`](VectorQuery.md)
#### See
[VectorQuery#refineFactor](VectorQuery.md#refinefactor)).  This means that setting a higher refine
factor can often help restore some of the results lost by post filtering.
***
### refineFactor()
```ts
refineFactor(refineFactor): VectorQuery
```
A multiplier to control how many additional rows are taken during the refine step
This argument is only used when the vector column has an IVF PQ index.
If there is no index then this value is ignored.
An IVF PQ index stores compressed (quantized) values.  They query vector is compared
against these values and, since they are compressed, the comparison is inaccurate.
This parameter can be used to refine the results.  It can improve both improve recall
and correct the ordering of the nearest results.
To refine results LanceDb will first perform an ANN search to find the nearest
`limit` * `refine_factor` results.  In other words, if `refine_factor` is 3 and
`limit` is the default (10) then the first 30 results will be selected.  LanceDb
then fetches the full, uncompressed, values for these 30 results.  The results are
then reordered by the true distance and only the nearest 10 are kept.
Note: there is a difference between calling this method with a value of 1 and never
calling this method at all.  Calling this method with any value will have an impact
on your search latency.  When you call this method with a `refine_factor` of 1 then
LanceDb still needs to fetch the full, uncompressed, values so that it can potentially
reorder the results.
Note: if this method is NOT called then the distances returned in the _distance column
will be approximate distances based on the comparison of the quantized query vector
and the quantized result vectors.  This can be considerably different than the true
distance between the query vector and the actual uncompressed vector.
#### Parameters
* **refineFactor**: `number`
#### Returns
[`VectorQuery`](VectorQuery.md)
***
### rerank()
```ts
rerank(reranker): VectorQuery
```
#### Parameters
* **reranker**: [`Reranker`](../namespaces/rerankers/interfaces/Reranker.md)
#### Returns
[`VectorQuery`](VectorQuery.md)
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
[`QueryBase`](QueryBase.md).[`where`](QueryBase.md#where)
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
