[vectordb](../README.md) / [Exports](../modules.md) / Connection

# Interface: Connection

A LanceDB Connection that allows you to open tables and create new ones.

Connection could be local against filesystem or remote against a server.

## Implemented by

- [`LocalConnection`](../classes/LocalConnection.md)

## Table of contents

### Properties

- [uri](Connection.md#uri)

### Methods

- [createTable](Connection.md#createtable)
- [dropTable](Connection.md#droptable)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)

## Properties

### uri

• **uri**: `string`

#### Defined in

[index.ts:125](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L125)

## Methods

### createTable

▸ **createTable**\<`T`\>(`«destructured»`): `Promise`\<[`Table`](Table.md)\<`T`\>\>

Creates a new Table, optionally initializing it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `«destructured»` | [`CreateTableOptions`](CreateTableOptions.md)\<`T`\> |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:146](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L146)

▸ **createTable**(`name`, `data`): `Promise`\<[`Table`](Table.md)\<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |

#### Returns

`Promise`\<[`Table`](Table.md)\<`number`[]\>\>

#### Defined in

[index.ts:154](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L154)

▸ **createTable**(`name`, `data`, `options`): `Promise`\<[`Table`](Table.md)\<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `options` | [`WriteOptions`](WriteOptions.md) | The write options to use when creating the table. |

#### Returns

`Promise`\<[`Table`](Table.md)\<`number`[]\>\>

#### Defined in

[index.ts:163](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L163)

▸ **createTable**\<`T`\>(`name`, `data`, `embeddings`): `Promise`\<[`Table`](Table.md)\<`T`\>\>

Creates a new Table and initialize it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\> | An embedding function to use on this table |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:172](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L172)

▸ **createTable**\<`T`\>(`name`, `data`, `embeddings`, `options`): `Promise`\<[`Table`](Table.md)\<`T`\>\>

Creates a new Table and initialize it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\> | An embedding function to use on this table |
| `options` | [`WriteOptions`](WriteOptions.md) | The write options to use when creating the table. |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:181](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L181)

___

### dropTable

▸ **dropTable**(`name`): `Promise`\<`void`\>

Drop an existing table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table to drop. |

#### Returns

`Promise`\<`void`\>

#### Defined in

[index.ts:187](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L187)

___

### openTable

▸ **openTable**\<`T`\>(`name`, `embeddings?`): `Promise`\<[`Table`](Table.md)\<`T`\>\>

Open a table in the database.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `embeddings?` | [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\> | An embedding function to use on this table |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:135](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L135)

___

### tableNames

▸ **tableNames**(): `Promise`\<`string`[]\>

#### Returns

`Promise`\<`string`[]\>

#### Defined in

[index.ts:127](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L127)
