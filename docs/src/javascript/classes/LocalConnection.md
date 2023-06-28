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
- [\_uri](LocalConnection.md#_uri)

### Accessors

- [uri](LocalConnection.md#uri)

### Methods

- [createTable](LocalConnection.md#createtable)
- [createTableArrow](LocalConnection.md#createtablearrow)
- [dropTable](LocalConnection.md#droptable)
- [openTable](LocalConnection.md#opentable)
- [tableNames](LocalConnection.md#tablenames)

## Constructors

### constructor

• **new LocalConnection**(`db`, `uri`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `db` | `any` |
| `uri` | `string` |

#### Defined in

[index.ts:129](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L129)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:127](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L127)

___

### \_uri

• `Private` `Readonly` **\_uri**: `string`

#### Defined in

[index.ts:126](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L126)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Implementation of

[Connection](../interfaces/Connection.md).[uri](../interfaces/Connection.md#uri)

#### Defined in

[index.ts:134](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L134)

## Methods

### createTable

▸ **createTable**(`name`, `data`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Record`<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the Table |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

Connection.createTable

#### Defined in

[index.ts:174](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L174)

▸ **createTable**<`T`\>(`name`, `data`, `embeddings`): `Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

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
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Implementation of

Connection.createTable

#### Defined in

[index.ts:182](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L182)

___

### createTableArrow

▸ **createTableArrow**(`name`, `table`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `table` | `Table`<`any`\> |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

[Connection](../interfaces/Connection.md).[createTableArrow](../interfaces/Connection.md#createtablearrow)

#### Defined in

[index.ts:192](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L192)

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

[index.ts:202](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L202)

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

Connection.openTable

#### Defined in

[index.ts:150](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L150)

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

[index.ts:157](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L157)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[index.ts:141](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L141)
