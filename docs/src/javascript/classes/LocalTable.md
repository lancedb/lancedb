[vectordb](../README.md) / [Exports](../modules.md) / LocalTable

# Class: LocalTable<T\>

A LanceDB table that allows you to search and update a table.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implements

- [`Table`](../interfaces/Table.md)<`T`\>

## Table of contents

### Constructors

- [constructor](LocalTable.md#constructor)

### Properties

- [\_embeddings](LocalTable.md#_embeddings)
- [\_name](LocalTable.md#_name)
- [\_tbl](LocalTable.md#_tbl)

### Accessors

- [name](LocalTable.md#name)

### Methods

- [add](LocalTable.md#add)
- [countRows](LocalTable.md#countrows)
- [createIndex](LocalTable.md#createindex)
- [delete](LocalTable.md#delete)
- [overwrite](LocalTable.md#overwrite)
- [search](LocalTable.md#search)

## Constructors

### constructor

• **new LocalTable**<`T`\>(`tbl`, `name`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `tbl` | `any` |
| `name` | `string` |

#### Defined in

[index.ts:243](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L243)

• **new LocalTable**<`T`\>(`tbl`, `name`, `embeddings`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `tbl` | `any` |  |
| `name` | `string` |  |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use when interacting with this table |

#### Defined in

[index.ts:249](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L249)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\>

#### Defined in

[index.ts:241](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L241)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:240](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L240)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:239](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L239)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[index.ts:256](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L256)

## Methods

### add

▸ **add**(`data`): `Promise`<`number`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[add](../interfaces/Table.md#add)

#### Defined in

[index.ts:274](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L274)

___

### countRows

▸ **countRows**(): `Promise`<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[index.ts:300](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L300)

___

### createIndex

▸ **createIndex**(`indexParams`): `Promise`<`any`\>

Create an ANN index on this Table vector index.

**`See`**

VectorIndexParams.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | `IvfPQIndexConfig` | The parameters of this Index, |

#### Returns

`Promise`<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[createIndex](../interfaces/Table.md#createindex)

#### Defined in

[index.ts:293](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L293)

___

### delete

▸ **delete**(`filter`): `Promise`<`void`\>

Delete rows from this table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. |

#### Returns

`Promise`<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[delete](../interfaces/Table.md#delete)

#### Defined in

[index.ts:309](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L309)

___

### overwrite

▸ **overwrite**(`data`): `Promise`<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[index.ts:284](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L284)

___

### search

▸ **search**(`query`): [`Query`](Query.md)<`T`\>

Creates a search query to find the nearest neighbors of the given search term

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

#### Returns

[`Query`](Query.md)<`T`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#search)

#### Defined in

[index.ts:264](https://github.com/lancedb/lancedb/blob/0162b16/node/src/index.ts#L264)
