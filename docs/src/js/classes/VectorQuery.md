[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / VectorQuery

# Class: VectorQuery

A builder used to construct a vector search

This builder can be reused to execute the query many times.

## Hierarchy

- [`QueryBase`](QueryBase.md)\<`NativeVectorQuery`, [`VectorQuery`](VectorQuery.md)\>

  ↳ **`VectorQuery`**

## Table of contents

### Constructors

- [constructor](VectorQuery.md#constructor)

### Properties

- [inner](VectorQuery.md#inner)

### Methods

- [[asyncIterator]](VectorQuery.md#[asynciterator])
- [bypassVectorIndex](VectorQuery.md#bypassvectorindex)
- [column](VectorQuery.md#column)
- [distanceType](VectorQuery.md#distancetype)
- [execute](VectorQuery.md#execute)
- [limit](VectorQuery.md#limit)
- [nativeExecute](VectorQuery.md#nativeexecute)
- [nprobes](VectorQuery.md#nprobes)
- [postfilter](VectorQuery.md#postfilter)
- [refineFactor](VectorQuery.md#refinefactor)
- [select](VectorQuery.md#select)
- [toArray](VectorQuery.md#toarray)
- [toArrow](VectorQuery.md#toarrow)
- [where](VectorQuery.md#where)

## Constructors

### constructor

• **new VectorQuery**(`inner`): [`VectorQuery`](VectorQuery.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `inner` | `VectorQuery` |

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Overrides

[QueryBase](QueryBase.md).[constructor](QueryBase.md#constructor)

#### Defined in

[query.ts:189](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L189)

## Properties

### inner

• `Protected` **inner**: `VectorQuery`

#### Inherited from

[QueryBase](QueryBase.md).[inner](QueryBase.md#inner)

#### Defined in

[query.ts:59](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L59)

## Methods

### [asyncIterator]

▸ **[asyncIterator]**(): `AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Returns

`AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Inherited from

[QueryBase](QueryBase.md).[[asyncIterator]](QueryBase.md#[asynciterator])

#### Defined in

[query.ts:154](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L154)

___

### bypassVectorIndex

▸ **bypassVectorIndex**(): [`VectorQuery`](VectorQuery.md)

If this is called then any vector index is skipped

An exhaustive (flat) search will be performed.  The query vector will
be compared to every vector in the table.  At high scales this can be
expensive.  However, this is often still useful.  For example, skipping
the vector index can give you ground truth results which you can use to
calculate your recall to select an appropriate value for nprobes.

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Defined in

[query.ts:321](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L321)

___

### column

▸ **column**(`column`): [`VectorQuery`](VectorQuery.md)

Set the vector column to query

This controls which column is compared to the query vector supplied in
the call to

#### Parameters

| Name | Type |
| :------ | :------ |
| `column` | `string` |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`See`**

[Query#nearestTo](Query.md#nearestto)

This parameter must be specified if the table has more than one column
whose data type is a fixed-size-list of floats.

#### Defined in

[query.ts:229](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L229)

___

### distanceType

▸ **distanceType**(`distanceType`): [`VectorQuery`](VectorQuery.md)

Set the distance metric to use

When performing a vector search we try and find the "nearest" vectors according
to some kind of distance metric.  This parameter controls which distance metric to
use.  See

#### Parameters

| Name | Type |
| :------ | :------ |
| `distanceType` | `string` |

#### Returns

[`VectorQuery`](VectorQuery.md)

**`See`**

[IvfPqOptions.distanceType](../interfaces/IvfPqOptions.md#distancetype) for more details on the different
distance metrics available.

Note: if there is a vector index then the distance type used MUST match the distance
type used to train the vector index.  If this is not done then the results will be
invalid.

By default "l2" is used.

#### Defined in

[query.ts:248](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L248)

___

### execute

▸ **execute**(): [`RecordBatchIterator`](RecordBatchIterator.md)

Execute the query and return the results as an

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

[query.ts:149](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L149)

___

### limit

▸ **limit**(`limit`): [`VectorQuery`](VectorQuery.md)

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

| Name | Type |
| :------ | :------ |
| `limit` | `number` |

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Inherited from

[QueryBase](QueryBase.md).[limit](QueryBase.md#limit)

#### Defined in

[query.ts:129](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L129)

___

### nativeExecute

▸ **nativeExecute**(): `Promise`\<`RecordBatchIterator`\>

#### Returns

`Promise`\<`RecordBatchIterator`\>

#### Inherited from

[QueryBase](QueryBase.md).[nativeExecute](QueryBase.md#nativeexecute)

#### Defined in

[query.ts:134](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L134)

___

### nprobes

▸ **nprobes**(`nprobes`): [`VectorQuery`](VectorQuery.md)

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

| Name | Type |
| :------ | :------ |
| `nprobes` | `number` |

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Defined in

[query.ts:215](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L215)

___

### postfilter

▸ **postfilter**(): [`VectorQuery`](VectorQuery.md)

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

**`See`**

[VectorQuery#refineFactor](VectorQuery.md#refinefactor)).  This means that setting a higher refine
factor can often help restore some of the results lost by post filtering.

#### Defined in

[query.ts:307](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L307)

___

### refineFactor

▸ **refineFactor**(`refineFactor`): [`VectorQuery`](VectorQuery.md)

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

| Name | Type |
| :------ | :------ |
| `refineFactor` | `number` |

#### Returns

[`VectorQuery`](VectorQuery.md)

#### Defined in

[query.ts:282](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L282)

___

### select

▸ **select**(`columns`): [`VectorQuery`](VectorQuery.md)

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
| `columns` | `string`[] \| `Record`\<`string`, `string`\> \| `Map`\<`string`, `string`\> |

#### Returns

[`VectorQuery`](VectorQuery.md)

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

[query.ts:108](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L108)

___

### toArray

▸ **toArray**(): `Promise`\<`unknown`[]\>

Collect the results as an array of objects.

#### Returns

`Promise`\<`unknown`[]\>

#### Inherited from

[QueryBase](QueryBase.md).[toArray](QueryBase.md#toarray)

#### Defined in

[query.ts:169](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L169)

___

### toArrow

▸ **toArrow**(): `Promise`\<`Table`\<`any`\>\>

Collect the results as an Arrow

#### Returns

`Promise`\<`Table`\<`any`\>\>

**`See`**

ArrowTable.

#### Inherited from

[QueryBase](QueryBase.md).[toArrow](QueryBase.md#toarrow)

#### Defined in

[query.ts:160](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L160)

___

### where

▸ **where**(`predicate`): [`VectorQuery`](VectorQuery.md)

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

[`VectorQuery`](VectorQuery.md)

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

[query.ts:73](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L73)
