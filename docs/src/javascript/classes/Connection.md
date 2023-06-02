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

• **new Connection**(`db`, `uri`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `db` | `any` |
| `uri` | `string` |

#### Defined in

[index.ts:46](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L46)

## Properties

### \_db

• `Private` `Readonly` **\_db**: `any`

#### Defined in

[index.ts:44](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L44)

___

### \_uri

• `Private` `Readonly` **\_uri**: `string`

#### Defined in

[index.ts:43](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L43)

## Accessors

### uri

• `get` **uri**(): `string`

#### Returns

`string`

#### Defined in

[index.ts:51](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L51)

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

[index.ts:91](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L91)

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
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:99](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L99)

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

[index.ts:109](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L109)

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

[index.ts:67](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L67)

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
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use on this Table |

#### Returns

`Promise`<[`Table`](Table.md)<`T`\>\>

#### Defined in

[index.ts:74](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L74)

___

### tableNames

▸ **tableNames**(): `Promise`<`string`[]\>

Get the names of all tables in the database.

#### Returns

`Promise`<`string`[]\>

#### Defined in

[index.ts:58](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L58)
