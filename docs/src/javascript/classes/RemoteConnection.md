[vectordb](../README.md) / [Exports](../saas-modules.md) / RemoteConnection

# Class: RemoteConnection

A connection to a remote LanceDB database. The class RemoteConnection implements interface Connection

## Implements

- [`Connection`](../interfaces/Connection.md)

## Table of contents

### Constructors

- [constructor](RemoteConnection.md#constructor)

### Methods

- [createTable](RemoteConnection.md#createtable)
- [tableNames](RemoteConnection.md#tablenames)
- [openTable](RemoteConnection.md#opentable)
- [dropTable](RemoteConnection.md#droptable)


## Constructors

### constructor

• **new RemoteConnection**(`client`, `dbName`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `client` | `HttpLancedbClient` |
| `dbName` | `string` |

#### Defined in

[remote/index.ts:37](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L37)

## Methods

### createTable

▸ **createTable**(`name`, `data`, `mode?`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the Table |
| `mode?` | [`WriteMode`](../enums/WriteMode.md) | The write mode to use when creating the table. |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[createTable](../interfaces/Connection.md#createtable)

#### Defined in

[remote/index.ts:75](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L75)

▸ **createTable**(`name`, `data`, `mode`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `data` | `Record`<`string`, `unknown`\>[] |
| `mode` | [`WriteMode`](../enums/WriteMode.md) |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

Connection.createTable

#### Defined in

[remote/index.ts:231](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L231)

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

#### Implementation of

[Connection](../interfaces/Connection.md).[dropTable](../interfaces/Connection.md#droptable)

#### Defined in

[remote/index.ts:131](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L131)

___

### openTable

▸ **openTable**(`name`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[openTable](../interfaces/Connection.md#opentable)

#### Defined in

[remote/index.ts:65](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L65)

▸ **openTable**<`T`\>(`name`, `embeddings`): `Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

Open a table in the database.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Implementation of

Connection.openTable

#### Defined in

[remote/index.ts:66](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L66)

▸ **openTable**<`T`\>(`name`, `embeddings?`): `Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `embeddings?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Implementation of

Connection.openTable

#### Defined in

[remote/index.ts:67](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L67)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database, with pagination.

#### Parameters

| Name | Type |
| :------ | :------ |
| `pageToken` | `string` |
| `limit` | `int` |

#### Returns

`Promise`<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[remote/index.ts:60](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L60)
