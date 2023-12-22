[vectordb](../README.md) / [Exports](../modules.md) / Table

# Interface: Table\<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implemented by

- [`LocalTable`](../classes/LocalTable.md)

## Table of contents

### Properties

- [add](Table.md#add)
- [countRows](Table.md#countrows)
- [createIndex](Table.md#createindex)
- [delete](Table.md#delete)
- [indexStats](Table.md#indexstats)
- [listIndices](Table.md#listindices)
- [name](Table.md#name)
- [overwrite](Table.md#overwrite)
- [search](Table.md#search)
- [update](Table.md#update)

## Properties

### add

• **add**: (`data`: `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:209](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L209)

___

### countRows

• **countRows**: () => `Promise`\<`number`\>

#### Type declaration

▸ (): `Promise`\<`number`\>

Returns the number of rows in this table.

##### Returns

`Promise`\<`number`\>

#### Defined in

[index.ts:229](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L229)

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

[index.ts:224](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L224)

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

[index.ts:263](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L263)

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

[index.ts:306](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L306)

___

### listIndices

• **listIndices**: () => `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Type declaration

▸ (): `Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

List the indicies on this table.

##### Returns

`Promise`\<[`VectorIndex`](VectorIndex.md)[]\>

#### Defined in

[index.ts:301](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L301)

___

### name

• **name**: `string`

#### Defined in

[index.ts:195](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L195)

___

### overwrite

• **overwrite**: (`data`: `Record`\<`string`, `unknown`\>[]) => `Promise`\<`number`\>

#### Type declaration

▸ (`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:217](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L217)

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

[index.ts:201](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L201)

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
  filter: "id = 2",
  updates: { vector: [2, 2], name: "Michael" },
})

let results = await tbl.search([1, 1]).execute();
// Returns [
//   {id: 2, vector: [2, 2], name: 'Michael'}
//   {id: 1, vector: [3, 3], name: 'Ye'}
// ]
```

#### Defined in

[index.ts:296](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L296)
