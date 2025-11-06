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
- [withMiddleware](Connection.md#withmiddleware)

## Properties

### uri

• **uri**: `string`

#### Defined in

[index.ts:261](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L261)

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

[index.ts:285](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L285)

▸ **createTable**(`name`, `data`): `Promise`\<[`Table`](Table.md)\<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |

#### Returns

`Promise`\<[`Table`](Table.md)\<`number`[]\>\>

#### Defined in

[index.ts:299](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L299)

▸ **createTable**(`name`, `data`, `options`): `Promise`\<[`Table`](Table.md)\<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `options` | [`WriteOptions`](WriteOptions.md) | The write options to use when creating the table. |

#### Returns

`Promise`\<[`Table`](Table.md)\<`number`[]\>\>

#### Defined in

[index.ts:311](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L311)

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
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\> | An embedding function to use on this table |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:324](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L324)

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
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `embeddings` | [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\> | An embedding function to use on this table |
| `options` | [`WriteOptions`](WriteOptions.md) | The write options to use when creating the table. |

#### Returns

`Promise`\<[`Table`](Table.md)\<`T`\>\>

#### Defined in

[index.ts:337](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L337)

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

[index.ts:348](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L348)

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

[index.ts:271](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L271)

___

### tableNames

▸ **tableNames**(): `Promise`\<`string`[]\>

#### Returns

`Promise`\<`string`[]\>

#### Defined in

[index.ts:263](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L263)

___

### withMiddleware

▸ **withMiddleware**(`middleware`): [`Connection`](Connection.md)

Instrument the behavior of this Connection with middleware.

The middleware will be called in the order they are added.

Currently this functionality is only supported for remote Connections.

#### Parameters

| Name | Type |
| :------ | :------ |
| `middleware` | `HttpMiddleware` |

#### Returns

[`Connection`](Connection.md)

- this Connection instrumented by the passed middleware

#### Defined in

[index.ts:360](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L360)
