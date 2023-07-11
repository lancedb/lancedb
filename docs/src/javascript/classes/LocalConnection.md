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

[index.ts:132](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L132)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:130](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L130)

___

### \_uri

• `Private` `Readonly` **\_uri**: `string`

#### Defined in

[index.ts:129](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L129)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Implementation of

[Connection](../interfaces/Connection.md).[uri](../interfaces/Connection.md#uri)

#### Defined in

[index.ts:137](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L137)

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

[index.ts:177](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L177)

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

[index.ts:178](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L178)

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

[index.ts:188](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L188)

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

[index.ts:201](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L201)

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

[index.ts:211](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L211)

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

[index.ts:153](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L153)

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

[index.ts:160](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L160)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`<`string`[]\>

#### Implementation of

[Connection](../interfaces/Connection.md).[tableNames](../interfaces/Connection.md#tablenames)

#### Defined in

[index.ts:144](https://github.com/lancedb/lancedb/blob/7247834/node/src/index.ts#L144)
