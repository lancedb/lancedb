[vectordb](../README.md) / [Exports](../modules.md) / RemoteConnection

# Class: RemoteConnection

Remote connection.

## Implements

- [`Connection`](../interfaces/Connection.md)

## Table of contents

### Constructors

- [constructor](RemoteConnection.md#constructor)

### Properties

- [\_client](RemoteConnection.md#_client)
- [\_dbName](RemoteConnection.md#_dbname)

### Accessors

- [uri](RemoteConnection.md#uri)

### Methods

- [createTable](RemoteConnection.md#createtable)
- [dropTable](RemoteConnection.md#droptable)
- [openTable](RemoteConnection.md#opentable)
- [tableNames](RemoteConnection.md#tablenames)

## Constructors

### constructor

• **new RemoteConnection**(`opts`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `opts` | [`ConnectionOptions`](../interfaces/ConnectionOptions.md) |

#### Defined in

[remote/index.ts:48](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L48)

## Properties

### \_client

• `Private` `Readonly` **\_client**: `HttpLancedbClient`

#### Defined in

[remote/index.ts:45](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L45)

___

### \_dbName

• `Private` `Readonly` **\_dbName**: `string`

#### Defined in

[remote/index.ts:46](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L46)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Implementation of

[Connection](../interfaces/Connection.md).[uri](../interfaces/Connection.md#uri)

#### Defined in

[remote/index.ts:75](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L75)

## Methods

### createTable

▸ **createTable**\<`T`\>(`nameOrOpts`, `data?`, `optsOrEmbedding?`, `opt?`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

Creates a new Table, optionally initializing it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `nameOrOpts` | `string` \| [`CreateTableOptions`](../interfaces/CreateTableOptions.md)\<`T`\> |
| `data?` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] |
| `optsOrEmbedding?` | [`WriteOptions`](../interfaces/WriteOptions.md) \| [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |
| `opt?` | [`WriteOptions`](../interfaces/WriteOptions.md) |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[createTable](../interfaces/Connection.md#createtable)

#### Defined in

[remote/index.ts:107](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L107)

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

#### Implementation of

[Connection](../interfaces/Connection.md).[dropTable](../interfaces/Connection.md#droptable)

#### Defined in

[remote/index.ts:175](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L175)

___

### openTable

▸ **openTable**(`name`): `Promise`\<[`Table`](../interfaces/Table.md)\<`number`[]\>\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`number`[]\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[openTable](../interfaces/Connection.md#opentable)

#### Defined in

[remote/index.ts:91](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L91)

▸ **openTable**\<`T`\>(`name`, `embeddings`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Implementation of

Connection.openTable

#### Defined in

[remote/index.ts:92](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L92)

___

### tableNames

▸ **tableNames**(`pageToken?`, `limit?`): `Promise`\<`string`[]\>

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `pageToken` | `string` | `''` |
| `limit` | `number` | `10` |

#### Returns

`Promise`\<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[remote/index.ts:80](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/remote/index.ts#L80)
