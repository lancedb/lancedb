[vectordb](../README.md) / [Exports](../modules.md) / Table

# Class: Table<T\>

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Table of contents

### Constructors

- [constructor](Table.md#constructor)

### Properties

- [\_embeddings](Table.md#_embeddings)
- [\_name](Table.md#_name)
- [\_tbl](Table.md#_tbl)

### Accessors

- [name](Table.md#name)

### Methods

- [add](Table.md#add)
- [create\_index](Table.md#create_index)
- [overwrite](Table.md#overwrite)
- [search](Table.md#search)

## Constructors

### constructor

• **new Table**<`T`\>(`tbl`, `name`)

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

[index.ts:121](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L121)

• **new Table**<`T`\>(`tbl`, `name`, `embeddings`)

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

[index.ts:127](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L127)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\>

#### Defined in

[index.ts:119](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L119)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:118](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L118)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:117](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L117)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Defined in

[index.ts:134](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L134)

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

#### Defined in

[index.ts:152](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L152)

___

### create\_index

▸ **create_index**(`indexParams`): `Promise`<`any`\>

Create an ANN index on this Table vector index.

**`See`**

VectorIndexParams.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexParams` | `IvfPQIndexConfig` | The parameters of this Index, |

#### Returns

`Promise`<`any`\>

#### Defined in

[index.ts:171](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L171)

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

#### Defined in

[index.ts:162](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L162)

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

#### Defined in

[index.ts:142](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L142)
