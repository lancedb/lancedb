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
- [createTableArrow](LocalConnection.md#createtablearrow)
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

[index.ts:184](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L184)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:182](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L182)

___

### \_options

• `Private` `Readonly` **\_options**: [`ConnectionOptions`](../interfaces/ConnectionOptions.md)

#### Defined in

[index.ts:181](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L181)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Implementation of

[Connection](../interfaces/Connection.md).[uri](../interfaces/Connection.md#uri)

#### Defined in

[index.ts:189](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L189)

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

[index.ts:230](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L230)

▸ **createTable**(`name`, `data`, `mode`): `Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `data` | `Record`<`string`, `unknown`\>[] |
| `mode` | [`WriteMode`](../enums/WriteMode.md) |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`number`[]\>\>

#### Implementation of

Connection.createTable

#### Defined in

[index.ts:231](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L231)

▸ **createTable**<`T`\>(`name`, `data`, `mode`, `embeddings`): `Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

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
| `mode` | [`WriteMode`](../enums/WriteMode.md) | The write mode to use when creating the table. |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Implementation of

Connection.createTable

#### Defined in

[index.ts:241](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L241)

▸ **createTable**<`T`\>(`name`, `data`, `mode`, `embeddings?`): `Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `data` | `Record`<`string`, `unknown`\>[] |
| `mode` | [`WriteMode`](../enums/WriteMode.md) |
| `embeddings?` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> |

#### Returns

`Promise`<[`Table`](../interfaces/Table.md)<`T`\>\>

#### Implementation of

Connection.createTable

#### Defined in

[index.ts:242](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L242)

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

[index.ts:266](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L266)

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

[index.ts:276](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L276)

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

[index.ts:205](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L205)

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

[index.ts:212](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L212)

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

[index.ts:213](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L213)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[index.ts:196](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L196)
