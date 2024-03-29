[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / Query

# Class: Query

A builder for LanceDB queries.

## Hierarchy

- [`QueryBase`](QueryBase.md)\<`NativeQuery`, [`Query`](Query.md)\>

  ↳ **`Query`**

## Table of contents

### Constructors

- [constructor](Query.md#constructor)

### Properties

- [inner](Query.md#inner)

### Methods

- [[asyncIterator]](Query.md#[asynciterator])
- [execute](Query.md#execute)
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

[query.ts:329](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L329)

## Properties

### inner

• `Protected` **inner**: `Query`

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

▸ **limit**(`limit`): [`Query`](Query.md)

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

| Name | Type |
| :------ | :------ |
| `limit` | `number` |

#### Returns

[`Query`](Query.md)

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
| `vector` | `unknown` |

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

[query.ts:370](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L370)

___

### select

▸ **select**(`columns`): [`Query`](Query.md)

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

[`Query`](Query.md)

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

▸ **where**(`predicate`): [`Query`](Query.md)

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

[`Query`](Query.md)

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
