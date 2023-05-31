[vectordb](../README.md) / [Exports](../modules.md) / Connection

# Class: Connection

A connection to a LanceDB database.

## Table of contents

### Constructors

- [constructor](Connection.md#constructor)

### Properties

- [\_db](Connection.md#_db)
- [\_uri](Connection.md#_uri)

### Accessors

- [uri](Connection.md#uri)

### Methods

- [createTable](Connection.md#createtable)
- [createTableArrow](Connection.md#createtablearrow)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)

## Constructors

### constructor

• **new Connection**(`uri`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `uri` | `string` |

#### Defined in

[index.ts:41](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L41)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:39](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L39)

___

### \_uri

• `Private` `Readonly` **\_uri**: `string`

#### Defined in

[index.ts:38](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L38)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Defined in

[index.ts:46](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L46)

## Methods

### createTable

▸ **createTable**(`name`, `data`): `Promise`<[`Table`](Table.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `data` | `Record`<`string`, `unknown`\>[] |

#### Returns

`Promise`<[`Table`](Table.md)\>

#### Defined in

[index.ts:66](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L66)

___

### createTableArrow

▸ **createTableArrow**(`name`, `table`): `Promise`<[`Table`](Table.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `table` | `Table`<`any`\> |

#### Returns

`Promise`<[`Table`](Table.md)\>

#### Defined in

[index.ts:71](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L71)

___

### openTable

▸ **openTable**(`name`): `Promise`<[`Table`](Table.md)\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |

#### Returns

`Promise`<[`Table`](Table.md)\>

#### Defined in

[index.ts:61](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L61)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`<`string`[]\>

#### Defined in

[index.ts:53](https://github.com/lancedb/lancedb/blob/6d6e80b/node/src/index.ts#L53)
