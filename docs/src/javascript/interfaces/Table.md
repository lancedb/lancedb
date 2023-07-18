[vectordb](../README.md) / [Exports](../modules.md) / Table

# Interface: Table<T\>

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
- [name](Table.md#name)
- [overwrite](Table.md#overwrite)
- [search](Table.md#search)

## Properties

### add

• **add**: (`data`: `Record`<`string`, `unknown`\>[]) => `Promise`<`number`\>

#### Type declaration

▸ (`data`): `Promise`<`number`\>

Insert records into this Table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:120](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L120)

___

### countRows

• **countRows**: () => `Promise`<`number`\>

#### Type declaration

▸ (): `Promise`<`number`\>

Returns the number of rows in this table.

##### Returns

`Promise`<`number`\>

#### Defined in

[index.ts:140](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L140)

___

### createIndex

• **createIndex**: (`indexParams`: [`IvfPQIndexConfig`](IvfPQIndexConfig.md)) => `Promise`<`any`\>

#### Type declaration

▸ (`indexParams`): `Promise`<`any`\>

Create an ANN index on this Table vector index.

**`See`**

VectorIndexParams.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | [`IvfPQIndexConfig`](IvfPQIndexConfig.md) | The parameters of this Index, |

##### Returns

`Promise`<`any`\>

#### Defined in

[index.ts:135](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L135)

___

### delete

• **delete**: (`filter`: `string`) => `Promise`<`void`\>

#### Type declaration

▸ (`filter`): `Promise`<`void`\>

Delete rows from this table.

This can be used to delete a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

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

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. The filter must not be empty. |

##### Returns

`Promise`<`void`\>

#### Defined in

[index.ts:174](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L174)

___

### name

• **name**: `string`

#### Defined in

[index.ts:106](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L106)

___

### overwrite

• **overwrite**: (`data`: `Record`<`string`, `unknown`\>[]) => `Promise`<`number`\>

#### Type declaration

▸ (`data`): `Promise`<`number`\>

Insert records into this Table, replacing its contents.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

##### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Defined in

[index.ts:128](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L128)

___

### search

• **search**: (`query`: `T`) => [`Query`](../classes/Query.md)<`T`\>

#### Type declaration

▸ (`query`): [`Query`](../classes/Query.md)<`T`\>

Creates a search query to find the nearest neighbors of the given search term

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

##### Returns

[`Query`](../classes/Query.md)<`T`\>

#### Defined in

[index.ts:112](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L112)
