[vectordb](../README.md) / [Exports](../modules.md) / LocalTable

# Class: LocalTable<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

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

[index.ts:221](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L221)

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

[index.ts:227](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L227)

## Properties

### \_embeddings

• `Private` `Optional` `Readonly` **\_embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\>

#### Defined in

[index.ts:219](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L219)

___

### \_name

• `Private` `Readonly` **\_name**: `string`

#### Defined in

[index.ts:218](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L218)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:217](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L217)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[index.ts:234](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L234)

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

[index.ts:252](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L252)

___

### countRows

▸ **countRows**(): `Promise`<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[index.ts:278](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L278)

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

[index.ts:271](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L271)

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

[index.ts:287](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L287)

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

[index.ts:262](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L262)

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

[index.ts:242](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L242)
