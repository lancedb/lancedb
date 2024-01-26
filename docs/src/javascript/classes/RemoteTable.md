[vectordb](../README.md) / [Exports](../modules.md) / RemoteTable

# Class: RemoteTable\<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implements

- [`Table`](../interfaces/Table.md)\<`T`\>

## Table of contents

### Constructors

- [constructor](RemoteTable.md#constructor)

### Properties

- [\_client](RemoteTable.md#_client)
- [\_embeddings](RemoteTable.md#_embeddings)
- [\_name](RemoteTable.md#_name)

### Accessors

- [name](RemoteTable.md#name)
- [schema](RemoteTable.md#schema)

### Methods

- [add](RemoteTable.md#add)
- [countRows](RemoteTable.md#countrows)
- [createIndex](RemoteTable.md#createindex)
- [createScalarIndex](RemoteTable.md#createscalarindex)
- [delete](RemoteTable.md#delete)
- [indexStats](RemoteTable.md#indexstats)
- [listIndices](RemoteTable.md#listindices)
- [overwrite](RemoteTable.md#overwrite)
- [search](RemoteTable.md#search)
- [update](RemoteTable.md#update)

## Constructors

### constructor

• **new RemoteTable**\<`T`\>(`client`, `name`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `client` | `HttpLancedbClient` |
| `name` | `string` |

#### Defined in

[remote/index.ts:234](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L234)

• **new RemoteTable**\<`T`\>(`client`, `name`, `embeddings`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `client` | `HttpLancedbClient` |
| `name` | `string` |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |

#### Defined in

[remote/index.ts:235](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L235)

## Properties

### \_client

• `Private` `Readonly` **\_client**: `HttpLancedbClient`

#### Defined in

[remote/index.ts:230](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L230)

___

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\>

#### Defined in

[remote/index.ts:231](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L231)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[remote/index.ts:232](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L232)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[remote/index.ts:250](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L250)

___

### schema

• `get` **schema**(): `Promise`\<`any`\>

#### Returns

`Promise`\<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[schema](../interfaces/Table.md#schema)

#### Defined in

[remote/index.ts:254](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L254)

## Methods

### add

▸ **add**(`data`): `Promise`\<`number`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[add](../interfaces/Table.md#add)

#### Defined in

[remote/index.ts:273](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L273)

___

### countRows

▸ **countRows**(): `Promise`\<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`\<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[remote/index.ts:372](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L372)

___

### createIndex

▸ **createIndex**(`indexParams`): `Promise`\<`void`\>

Create an ANN index on this Table vector index.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | [`IvfPQIndexConfig`](../interfaces/IvfPQIndexConfig.md) | The parameters of this Index, |

#### Returns

`Promise`\<`void`\>

**`See`**

VectorIndexParams.

#### Implementation of

[Table](../interfaces/Table.md).[createIndex](../interfaces/Table.md#createindex)

#### Defined in

[remote/index.ts:326](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L326)

___

### createScalarIndex

▸ **createScalarIndex**(`column`, `replace`): `Promise`\<`void`\>

Create a scalar index on this Table for the given column

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `column` | `string` | The column to index |
| `replace` | `boolean` | If false, fail if an index already exists on the column Scalar indices, like vector indices, can be used to speed up scans. A scalar index can speed up scans that contain filter expressions on the indexed column. For example, the following scan will be faster if the column `my_col` has a scalar index: ```ts const con = await lancedb.connect('./.lancedb'); const table = await con.openTable('images'); const results = await table.where('my_col = 7').execute(); ``` Scalar indices can also speed up scans containing a vector search and a prefilter: ```ts const con = await lancedb.connect('././lancedb'); const table = await con.openTable('images'); const results = await table.search([1.0, 2.0]).where('my_col != 7').prefilter(true); ``` Scalar indices can only speed up scans for basic filters using equality, comparison, range (e.g. `my_col BETWEEN 0 AND 100`), and set membership (e.g. `my_col IN (0, 1, 2)`) Scalar indices can be used if the filter contains multiple indexed columns and the filter criteria are AND'd or OR'd together (e.g. `my_col < 0 AND other_col> 100`) Scalar indices may be used if the filter contains non-indexed columns but, depending on the structure of the filter, they may not be usable. For example, if the column `not_indexed` does not have a scalar index then the filter `my_col = 0 OR not_indexed = 1` will not be able to use any scalar index on `my_col`. |

#### Returns

`Promise`\<`void`\>

**`Examples`**

```ts
const con = await lancedb.connect('././lancedb')
const table = await con.openTable('images')
await table.createScalarIndex('my_col')
```

#### Implementation of

[Table](../interfaces/Table.md).[createScalarIndex](../interfaces/Table.md#createscalarindex)

#### Defined in

[remote/index.ts:368](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L368)

___

### delete

▸ **delete**(`filter`): `Promise`\<`void`\>

Delete rows from this table.

This can be used to delete a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. The filter must not be empty. |

#### Returns

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

#### Implementation of

[Table](../interfaces/Table.md).[delete](../interfaces/Table.md#delete)

#### Defined in

[remote/index.ts:377](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L377)

___

### indexStats

▸ **indexStats**(`indexUuid`): `Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

Get statistics about an index.

#### Parameters

| Name | Type |
| :------ | :------ |
| `indexUuid` | `string` |

#### Returns

`Promise`\<[`IndexStats`](../interfaces/IndexStats.md)\>

#### Implementation of

[Table](../interfaces/Table.md).[indexStats](../interfaces/Table.md#indexstats)

#### Defined in

[remote/index.ts:414](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L414)

___

### listIndices

▸ **listIndices**(): `Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

List the indicies on this table.

#### Returns

`Promise`\<[`VectorIndex`](../interfaces/VectorIndex.md)[]\>

#### Implementation of

[Table](../interfaces/Table.md).[listIndices](../interfaces/Table.md#listindices)

#### Defined in

[remote/index.ts:403](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L403)

___

### overwrite

▸ **overwrite**(`data`): `Promise`\<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`\<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[remote/index.ts:300](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L300)

___

### search

▸ **search**(`query`): [`Query`](Query.md)\<`T`\>

Creates a search query to find the nearest neighbors of the given search term

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

#### Returns

[`Query`](Query.md)\<`T`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#search)

#### Defined in

[remote/index.ts:269](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L269)

___

### update

▸ **update**(`args`): `Promise`\<`void`\>

Update rows in this table.

This can be used to update a single row, many rows, all rows, or
sometimes no rows (if your predicate matches nothing).

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `args` | [`UpdateArgs`](../interfaces/UpdateArgs.md) \| [`UpdateSqlArgs`](../interfaces/UpdateSqlArgs.md) | see [UpdateArgs](../interfaces/UpdateArgs.md) and [UpdateSqlArgs](../interfaces/UpdateSqlArgs.md) for more details |

#### Returns

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

#### Implementation of

[Table](../interfaces/Table.md).[update](../interfaces/Table.md#update)

#### Defined in

[remote/index.ts:383](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L383)
