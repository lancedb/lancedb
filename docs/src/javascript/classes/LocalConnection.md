[vectordb](../README.md) / [Exports](../modules.md) / LocalConnection

# Class: LocalConnection

A connection to a LanceDB database.

## Implements

- [`Connection`](../interfaces/Connection.md)

## Table of contents

### Constructors

- [constructor](LocalConnection.md#constructor)

### Properties

- [\_db](LocalConnection.md#_db)
- [\_options](LocalConnection.md#_options)

### Accessors

- [uri](LocalConnection.md#uri)

### Methods

- [createTable](LocalConnection.md#createtable)
- [createTableImpl](LocalConnection.md#createtableimpl)
- [dropTable](LocalConnection.md#droptable)
- [openTable](LocalConnection.md#opentable)
- [tableNames](LocalConnection.md#tablenames)

## Constructors

### constructor

• **new LocalConnection**(`db`, `options`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `db` | `any` |
| `options` | [`ConnectionOptions`](../interfaces/ConnectionOptions.md) |

#### Defined in

[index.ts:489](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L489)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:487](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L487)

___

### \_options

• `Private` `Readonly` **\_options**: () => [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Type declaration

▸ (): [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

##### Returns

[`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Defined in

[index.ts:486](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L486)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Implementation of

[Connection](../interfaces/Connection.md).[uri](../interfaces/Connection.md#uri)

#### Defined in

[index.ts:494](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L494)

## Methods

### createTable

▸ **createTable**\<`T`\>(`name`, `data?`, `optsOrEmbedding?`, `opt?`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

Creates a new Table, optionally initializing it with new data.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` \| [`CreateTableOptions`](../interfaces/CreateTableOptions.md)\<`T`\> |
| `data?` | `Record`\<`string`, `unknown`\>[] |
| `optsOrEmbedding?` | [`WriteOptions`](../interfaces/WriteOptions.md) \| [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |
| `opt?` | [`WriteOptions`](../interfaces/WriteOptions.md) |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[createTable](../interfaces/Connection.md#createtable)

#### Defined in

[index.ts:542](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L542)

___

### createTableImpl

▸ `Private` **createTableImpl**\<`T`\>(`«destructured»`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `«destructured»` | `Object` |
| › `data?` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] |
| › `embeddingFunction?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |
| › `name` | `string` |
| › `schema?` | `Schema`\<`any`\> |
| › `writeOptions?` | [`WriteOptions`](../interfaces/WriteOptions.md) |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Defined in

[index.ts:576](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L576)

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

[index.ts:630](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L630)

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

[index.ts:510](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L510)

▸ **openTable**\<`T`\>(`name`, `embeddings`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

Open a table in the database.

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Implementation of

Connection.openTable

#### Defined in

[index.ts:518](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L518)

▸ **openTable**\<`T`\>(`name`, `embeddings?`): `Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `embeddings?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`T`\> |

#### Returns

`Promise`\<[`Table`](../interfaces/Table.md)\<`T`\>\>

#### Implementation of

Connection.openTable

#### Defined in

[index.ts:522](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L522)

___

### tableNames

▸ **tableNames**(): `Promise`\<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`\<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[index.ts:501](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L501)
