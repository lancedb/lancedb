[vectordb](../README.md) / [Exports](../modules.md) / Connection

# Interface: Connection

A LanceDB connection that allows you to open tables and create new ones.

Connection could be local against filesystem or remote against a server.

## Implemented by

- [`LocalConnection`](../classes/LocalConnection.md)

## Table of contents

### Properties

- [uri](Connection.md#uri)

### Methods

- [createTable](Connection.md#createtable)
- [createTableArrow](Connection.md#createtablearrow)
- [dropTable](Connection.md#droptable)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)

## Properties

### uri

• **uri**: `string`

#### Defined in

[index.ts:45](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L45)

## Methods

### createTable

▸ **createTable**(`name`, `data`): `Promise`<[`Table`](Table.md)<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the Table |

#### Returns

`Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Defined in

[index.ts:75](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L75)

▸ **createTable**<`T`\>(`name`, `data`, `embeddings`): `Promise`<[`Table`](Table.md)<`T`\>\>

Creates a new Table and initialize it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the Table |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:82](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L82)

▸ **createTable**<`T`\>(`name`, `data`, `embeddings?`): `Promise`<[`Table`](Table.md)<`T`\>\>

Creates a new Table and initialize it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the Table |
| `embeddings?` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:89](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L89)

___

### createTableArrow

▸ **createTableArrow**(`name`, `table`): `Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `table` | `Table`<`any`\> |

#### Returns

`Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Defined in

[index.ts:94](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L94)

___

### dropTable

▸ **dropTable**(`name`): `Promise`<`void`\>

Drop an existing table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table to drop. |

#### Returns

`Promise`<`void`\>

#### Defined in

[index.ts:100](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L100)

___

### openTable

▸ **openTable**(`name`): `Promise`<[`Table`](Table.md)<`number`[]\>\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |

#### Returns

`Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Defined in

[index.ts:56](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L56)

▸ **openTable**<`T`\>(`name`, `embeddings`): `Promise`<[`Table`](Table.md)<`T`\>\>

Open a table in the database.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:62](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L62)

▸ **openTable**<`T`\>(`name`, `embeddings?`): `Promise`<[`Table`](Table.md)<`T`\>\>

Open a table in the database.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `embeddings?` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:68](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L68)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

#### Returns

`Promise`<`string`[]\>

#### Defined in

[index.ts:50](https://github.com/lancedb/lancedb/blob/a6bdffd/node/src/index.ts#L50)
