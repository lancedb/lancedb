[vectordb](../README.md) / [Exports](../modules.md) / Connection

# Interface: Connection

A LanceDB connection that allows you to open tables and create new ones.

Connection could be local against filesystem or remote against a server.

## Implemented by

- [`LocalConnection`](../classes/LocalConnection.md)

## Table of contents

### Properties

- [createTable](Connection.md#createtable)
- [createTableArrow](Connection.md#createtablearrow)
- [dropTable](Connection.md#droptable)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)
- [uri](Connection.md#uri)

## Properties

### createTable

• **createTable**: (`name`: `string`, `data`: `Record`<`string`, `unknown`\>[]) => `Promise`<[`Table`](Table.md)<`number`[]\>\> & <T\>(`name`: `string`, `data`: `Record`<`string`, `unknown`\>[], `embeddings`: [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\>) => `Promise`<[`Table`](Table.md)<`T`\>\> & <T\>(`name`: `string`, `data`: `Record`<`string`, `unknown`\>[], `embeddings?`: [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\>) => `Promise`<[`Table`](Table.md)<`T`\>\>

Creates a new Table and initialize it with new data.

**`Param`**

The name of the table.

**`Param`**

Non-empty Array of Records to be inserted into the Table

#### Defined in

[index.ts:63](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L63)

___

### createTableArrow

• **createTableArrow**: (`name`: `string`, `table`: `Table`<`any`\>) => `Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Type declaration

▸ (`name`, `table`): `Promise`<[`Table`](Table.md)<`number`[]\>\>

##### Parameters

| Name | Type |
| :------ | :------ |
| `name` | `string` |
| `table` | `Table`<`any`\> |

##### Returns

`Promise`<[`Table`](Table.md)<`number`[]\>\>

#### Defined in

[index.ts:65](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L65)

___

### dropTable

• **dropTable**: (`name`: `string`) => `Promise`<`void`\>

#### Type declaration

▸ (`name`): `Promise`<`void`\>

Drop an existing table.

##### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table to drop. |

##### Returns

`Promise`<`void`\>

#### Defined in

[index.ts:71](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L71)

___

### openTable

• **openTable**: (`name`: `string`) => `Promise`<[`Table`](Table.md)<`number`[]\>\> & <T\>(`name`: `string`, `embeddings`: [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\>) => `Promise`<[`Table`](Table.md)<`T`\>\> & <T\>(`name`: `string`, `embeddings?`: [`EmbeddingFunction`](EmbeddingFunction.md)<`T`\>) => `Promise`<[`Table`](Table.md)<`T`\>\>

Open a table in the database.

**`Param`**

The name of the table.

#### Defined in

[index.ts:54](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L54)

___

### tableNames

• **tableNames**: () => `Promise`<`string`[]\>

#### Type declaration

▸ (): `Promise`<`string`[]\>

##### Returns

`Promise`<`string`[]\>

#### Defined in

[index.ts:47](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L47)

___

### uri

• **uri**: `string`

#### Defined in

[index.ts:45](https://github.com/lancedb/lancedb/blob/bfb5400/node/src/index.ts#L45)
