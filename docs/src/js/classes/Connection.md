[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / Connection

# Class: Connection

A LanceDB Connection that allows you to open tables and create new ones.

Connection could be local against filesystem or remote against a server.

A Connection is intended to be a long lived object and may hold open
resources such as HTTP connection pools.  This is generally fine and
a single connection should be shared if it is going to be used many
times. However, if you are finished with a connection, you may call
close to eagerly free these resources.  Any call to a Connection
method after it has been closed will result in an error.

Closing a connection is optional.  Connections will automatically
be closed when they are garbage collected.

Any created tables are independent and will continue to work even if
the underlying connection has been closed.

## Table of contents

### Constructors

- [constructor](Connection.md#constructor)

### Methods

- [close](Connection.md#close)
- [createEmptyTable](Connection.md#createemptytable)
- [createTable](Connection.md#createtable)
- [display](Connection.md#display)
- [dropTable](Connection.md#droptable)
- [isOpen](Connection.md#isopen)
- [openTable](Connection.md#opentable)
- [tableNames](Connection.md#tablenames)

## Constructors

### constructor

• **new Connection**(): [`Connection`](Connection.md)

#### Returns

[`Connection`](Connection.md)

## Methods

### close

▸ **close**(): `void`

Close the connection, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the connection after it is closed will result in an error.

#### Returns

`void`

#### Defined in

[connection.ts:128](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L128)

___

### createEmptyTable

▸ **createEmptyTable**(`name`, `schema`, `options?`): `Promise`\<[`Table`](Table.md)\>

Creates a new empty Table

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `schema` | `SchemaLike` | The schema of the table |
| `options?` | `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | - |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:184](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L184)

___

### createTable

▸ **createTable**(`options`): `Promise`\<[`Table`](Table.md)\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `options` | \{ `data`: [`Data`](../modules.md#data) ; `name`: `string`  } & `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | The options object. |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:161](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L161)

▸ **createTable**(`name`, `data`, `options?`): `Promise`\<[`Table`](Table.md)\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `TableLike` \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `options?` | `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | - |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:173](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L173)

___

### display

▸ **display**(): `string`

Return a brief description of the connection

#### Returns

`string`

#### Defined in

[connection.ts:133](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L133)

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

[connection.ts:194](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L194)

___

### isOpen

▸ **isOpen**(): `boolean`

Return true if the connection has not been closed

#### Returns

`boolean`

#### Defined in

[connection.ts:119](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L119)

___

### openTable

▸ **openTable**(`name`, `options?`): `Promise`\<[`Table`](Table.md)\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table |
| `options?` | `Partial`\<`OpenTableOptions`\> | - |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:149](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L149)

___

### tableNames

▸ **tableNames**(`options?`): `Promise`\<`string`[]\>

List all the table names in this database.

Tables will be returned in lexicographical order.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `options?` | `Partial`\<[`TableNamesOptions`](../interfaces/TableNamesOptions.md)\> | options to control the paging / start point |

#### Returns

`Promise`\<`string`[]\>

#### Defined in

[connection.ts:143](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L143)
