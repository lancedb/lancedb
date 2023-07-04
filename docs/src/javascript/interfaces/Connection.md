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
- [createTableArrow](Connection.md#createtablearrow)
- [dropTable](Connection.md#droptable)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)

## Properties

### uri

• **uri**: `string`

#### Defined in

[index.ts:45](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L45)

## Methods

### createTable

▸ **createTable**<`T`\>(`name`, `data`, `mode?`, `embeddings?`): `Promise`<[`Table`](Table.md)<`T`\>\>

Creates a new Table and initialize it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `mode?` | [`WriteMode`](../enums/WriteMode.md) | The write mode to use when creating the table. |
| `embeddings?` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:65](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L65)

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

[index.ts:67](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L67)

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

[index.ts:73](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L73)

___

### openTable

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
| `embeddings?` | [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\> | An embedding function to use on this table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:55](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L55)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

#### Returns

`Promise`<`string`[]\>

#### Defined in

[index.ts:47](https://github.com/lancedb/lancedb/blob/97101eb/node/src/index.ts#L47)
