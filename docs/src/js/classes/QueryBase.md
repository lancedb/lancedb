[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / QueryBase

# Class: QueryBase\<NativeQueryType, QueryType\>

Common methods supported by all query types

## Type parameters

| Name | Type |
| :------ | :------ |
| `NativeQueryType` | extends `NativeQuery` \| `NativeVectorQuery` |
| `QueryType` | `QueryType` |

## Hierarchy

- **`QueryBase`**

  ↳ [`Query`](Query.md)

  ↳ [`VectorQuery`](VectorQuery.md)

## Implements

- `AsyncIterable`\<`RecordBatch`\>

## Table of contents

### Constructors

- [constructor](QueryBase.md#constructor)

### Properties

- [inner](QueryBase.md#inner)

### Methods

- [[asyncIterator]](QueryBase.md#[asynciterator])
- [execute](QueryBase.md#execute)
- [limit](QueryBase.md#limit)
- [nativeExecute](QueryBase.md#nativeexecute)
- [select](QueryBase.md#select)
- [toArray](QueryBase.md#toarray)
- [toArrow](QueryBase.md#toarrow)
- [where](QueryBase.md#where)

## Constructors

### constructor

• **new QueryBase**\<`NativeQueryType`, `QueryType`\>(`inner`): [`QueryBase`](QueryBase.md)\<`NativeQueryType`, `QueryType`\>

#### Type parameters

| Name | Type |
| :------ | :------ |
| `NativeQueryType` | extends `Query` \| `VectorQuery` |
| `QueryType` | `QueryType` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `inner` | `NativeQueryType` |

#### Returns

[`QueryBase`](QueryBase.md)\<`NativeQueryType`, `QueryType`\>

#### Defined in

[query.ts:59](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L59)

## Properties

### inner

• `Protected` **inner**: `NativeQueryType`

#### Defined in

[query.ts:59](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L59)

## Methods

### [asyncIterator]

▸ **[asyncIterator]**(): `AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Returns

`AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Implementation of

AsyncIterable.[asyncIterator]

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

#### Defined in

[query.ts:149](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L149)

___

### limit

▸ **limit**(`limit`): `QueryType`

Set the maximum number of results to return.

By default, a plain search has no limit.  If this method is not
called then every valid row from the table will be returned.

#### Parameters

| Name | Type |
| :------ | :------ |
| `limit` | `number` |

#### Returns

`QueryType`

#### Defined in

[query.ts:129](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L129)

___

### nativeExecute

▸ **nativeExecute**(): `Promise`\<`RecordBatchIterator`\>

#### Returns

`Promise`\<`RecordBatchIterator`\>

#### Defined in

[query.ts:134](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L134)

___

### select

▸ **select**(`columns`): `QueryType`

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

`QueryType`

**`Example`**

```ts
new Map([["combined", "a + b"], ["c", "c"]])

Columns will always be returned in the order given, even if that order is different than
the order used when adding the data.

Note that you can pass in a `Record<string, string>` (e.g. an object literal). This method
uses `Object.entries` which should preserve the insertion order of the object.  However,
object insertion order is easy to get wrong and `Map` is more foolproof.
```

#### Defined in

[query.ts:108](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L108)

___

### toArray

▸ **toArray**(): `Promise`\<`unknown`[]\>

Collect the results as an array of objects.

#### Returns

`Promise`\<`unknown`[]\>

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

#### Defined in

[query.ts:160](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L160)

___

### where

▸ **where**(`predicate`): `QueryType`

A filter statement to be applied to this query.

The filter should be supplied as an SQL query string.  For example:

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | `string` |

#### Returns

`QueryType`

**`Example`**

```ts
x > 10
y > 0 AND y < 100
x > 5 OR y = 'test'

Filtering performance can often be improved by creating a scalar index
on the filter column(s).
```

#### Defined in

[query.ts:73](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/query.ts#L73)
