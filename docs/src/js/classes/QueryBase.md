[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / QueryBase

# Class: QueryBase\<NativeQueryType\>

Common methods supported by all query types

## Type parameters

| Name | Type |
| :------ | :------ |
| `NativeQueryType` | extends `NativeQuery` \| `NativeVectorQuery` |

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
- [doCall](QueryBase.md#docall)
- [execute](QueryBase.md#execute)
- [explainPlan](QueryBase.md#explainplan)
- [filter](QueryBase.md#filter)
- [limit](QueryBase.md#limit)
- [nativeExecute](QueryBase.md#nativeexecute)
- [select](QueryBase.md#select)
- [toArray](QueryBase.md#toarray)
- [toArrow](QueryBase.md#toarrow)
- [where](QueryBase.md#where)

## Constructors

### constructor

• **new QueryBase**\<`NativeQueryType`\>(`inner`): [`QueryBase`](QueryBase.md)\<`NativeQueryType`\>

#### Type parameters

| Name | Type |
| :------ | :------ |
| `NativeQueryType` | extends `Query` \| `VectorQuery` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `inner` | `NativeQueryType` \| `Promise`\<`NativeQueryType`\> |

#### Returns

[`QueryBase`](QueryBase.md)\<`NativeQueryType`\>

#### Defined in

[query.ts:95](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L95)

## Properties

### inner

• `Protected` **inner**: `NativeQueryType` \| `Promise`\<`NativeQueryType`\>

#### Defined in

[query.ts:96](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L96)

## Methods

### [asyncIterator]

▸ **[asyncIterator]**(): `AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Returns

`AsyncIterator`\<`RecordBatch`\<`any`\>, `any`, `undefined`\>

#### Implementation of

AsyncIterable.[asyncIterator]

#### Defined in

[query.ts:226](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L226)

___

### doCall

▸ **doCall**(`fn`): `void`

#### Parameters

| Name | Type |
| :------ | :------ |
| `fn` | (`inner`: `NativeQueryType`) => `void` |

#### Returns

`void`

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

#### Defined in

[query.ts:198](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L198)

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

#### Defined in

[query.ts:124](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/query.ts#L124)
