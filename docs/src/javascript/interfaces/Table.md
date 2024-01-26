[vectordb](../README.md) / [Exports](../modules.md) / Table

# Interface: Table\<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implemented by

- [`LocalTable`](../classes/LocalTable.md)
- [`RemoteTable`](../classes/RemoteTable.md)

## Table of contents

### Properties

- [add](Table.md#add)
- [countRows](Table.md#countrows)
- [createIndex](Table.md#createindex)
- [createScalarIndex](Table.md#createscalarindex)
- [delete](Table.md#delete)
- [indexStats](Table.md#indexstats)
- [listIndices](Table.md#listindices)
- [name](Table.md#name)
- [overwrite](Table.md#overwrite)
- [schema](Table.md#schema)
- [search](Table.md#search)
- [update](Table.md#update)

## Properties

### add

• **add**: (`data`: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:296](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L296)

___

### countRows

• **countRows**: () => `Promise`\<`number`\>

#### Type declaration

▸ (): `Promise`\<`number`\>

Returns the number of rows in this table.

##### Returns

`Promise`\<`number`\>

#### Defined in

[index.ts:368](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L368)

___

### createIndex

• **createIndex**: (`indexParams`: [`IvfPQIndexConfig`](IvfPQIndexConfig.md)) => `Promise`\<`any`\>

#### Type declaration

▸ (`indexParams`): `Promise`\<`any`\>

Create an ANN index on this Table vector index.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | [`IvfPQIndexConfig`](IvfPQIndexConfig.md) | The parameters of this Index, |

##### Returns

`Promise`\<`any`\>

**`See`**

VectorIndexParams.

#### Defined in

[index.ts:313](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L313)

___

### createScalarIndex

• **createScalarIndex**: (`column`: `string`, `replace`: `boolean`) => `Promise`\<`void`\>

#### Type declaration

▸ (`column`, `replace`): `Promise`\<`void`\>

Create a scalar index on this Table for the given column

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `column` | `string` | The column to index |
| `replace` | `boolean` | If false, fail if an index already exists on the column Scalar indices, like vector indices, can be used to speed up scans. A scalar index can speed up scans that contain filter expressions on the indexed column. For example, the following scan will be faster if the column `my_col` has a scalar index: ```ts const con = await lancedb.connect('./.lancedb'); const table = await con.openTable('images'); const results = await table.where('my_col = 7').execute(); ``` Scalar indices can also speed up scans containing a vector search and a prefilter: ```ts const con = await lancedb.connect('././lancedb'); const table = await con.openTable('images'); const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true); ``` Scalar indices can only speed up scans for basic filters using equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set membership (e.g. `my_col IN (0, 1, 2)`) Scalar indices can be used if the filter contains multiple indexed columns and the filter criteria are AND'd or OR'd together (e.g. `my_col < 0 AND other_col> 100`) Scalar indices may be used if the filter contains non-indexed columns but, depending on the structure of the filter, they may not be usable. For example, if the column `not_indexed` does not have a scalar index then the filter `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on `my_col`. |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect('././lancedb')
const table = await con.openTable('images')
await table.createScalarIndex('my_col')
```

#### Defined in

[index.ts:363](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L363)

___

### delete

• **delete**: (`filter`: `string`) => `Promise`\<`void`\>

#### Type declaration

▸ (`filter`): `Promise`\<`void`\>

Delete rows from this table.

This can be used to delete a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. The filter must not be empty. |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect("./.lancedb")
const data = [
   {id: 1, vector: [1, 2]},
   {id: 2, vector: [3, 4]},
   {id: 3, vector: [5, 6]},
];
const tbl = await con.createTable("my_table", data)
await tbl.delete("id = 2")
await tbl.countRows() // Returns 2
```

If you have a list of values to delete, you can combine them into a
stringified list and use the `IN` operator:

```ts
const to_remove = [1, 5];
await tbl.delete(`id IN (${to_remove.join(",")})`)
await tbl.countRows() // Returns 1
```

#### Defined in

[index.ts:402](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L402)

___

### indexStats

• **indexStats**: (`indexUuid`: `string`) => `Promise`\<[`IndexStats`](IndexStats.md)\>

#### Type declaration

▸ (`indexUuid`): `Promise`\<[`IndexStats`](IndexStats.md)\>

Get statistics about an index.

##### Parameters

| Name | Type |
| :------ | :------ |
| `indexUuid` | `string` |

##### Returns

`Promise`\<[`IndexStats`](IndexStats.md)\>

#### Defined in

[index.ts:445](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L445)

___

### listIndices

• **listIndices**: () => `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Type declaration

▸ (): `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

List the indicies on this table.

##### Returns

`Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Defined in

[index.ts:440](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L440)

___

### name

• **name**: `string`

#### Defined in

[index.ts:282](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L282)

___

### overwrite

• **overwrite**: (`data`: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:304](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L304)

___

### schema

• **schema**: `Promise`\<`Schema`\<`any`\>\>

#### Defined in

[index.ts:447](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L447)

___

### search

• **search**: (`query`: `T`) => [`Query`](../classes/Query.md)\<`T`\>

#### Type declaration

▸ (`query`): [`Query`](../classes/Query.md)\<`T`\>

Creates a search query to find the nearest neighbors of the given search term

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

##### Returns

[`Query`](../classes/Query.md)\<`T`\>

#### Defined in

[index.ts:288](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L288)

___

### update

• **update**: (`args`: [`UpdateArgs`](UpdateArgs.md) \| [`UpdateSqlArgs`](UpdateSqlArgs.md)) => `Promise`\<`void`\>

#### Type declaration

▸ (`args`): `Promise`\<`void`\>

Update rows in this table.

This can be used to update a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `args` | [`UpdateArgs`](UpdateArgs.md) \| [`UpdateSqlArgs`](UpdateSqlArgs.md) | see [UpdateArgs](UpdateArgs.md) and [UpdateSqlArgs](UpdateSqlArgs.md) for more details |

##### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect("./.lancedb")
const data = [
   {id: 1, vector: [3, 3], name: 'Ye'},
   {id: 2, vector: [4, 4], name: 'Mike'},
];
const tbl = await con.createTable("my_table", data)

await tbl.update({
  where: "id = 2",
  values: { vector: [2, 2], name: "Michael" },
})

let results = await tbl.search([1, 1]).execute();
// Returns [
//   {id: 2, vector: [2, 2], name: 'Michael'}
//   {id: 1, vector: [3, 3], name: 'Ye'}
// ]
```

#### Defined in

[index.ts:435](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L435)
