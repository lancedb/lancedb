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

### Properties

- [inner](Connection.md#inner)

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

• **new Connection**(`inner`): [`Connection`](Connection.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `inner` | `Connection` |

#### Returns

[`Connection`](Connection.md)

#### Defined in

[connection.ts:72](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L72)

## Properties

### inner

• `Readonly` **inner**: `Connection`

#### Defined in

[connection.ts:70](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L70)

## Methods

### close

▸ **close**(): `void`

Close the connection, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the connection after it is closed will result in an error.

#### Returns

`void`

#### Defined in

[connection.ts:88](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L88)

___

### createEmptyTable

▸ **createEmptyTable**(`name`, `schema`, `options?`): `Promise`\<[`Table`](Table.md)\>

Creates a new empty Table

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `schema` | `Schema`\<`any`\> | The schema of the table |
| `options?` | `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | - |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:151](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L151)

___

### createTable

▸ **createTable**(`name`, `data`, `options?`): `Promise`\<[`Table`](Table.md)\>

Creates a new Table and initialize it with new data.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table. |
| `data` | `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[] | Non-empty Array of Records to be inserted into the table |
| `options?` | `Partial`\<[`CreateTableOptions`](../interfaces/CreateTableOptions.md)\> | - |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:123](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L123)

___

### display

▸ **display**(): `string`

Return a brief description of the connection

#### Returns

`string`

#### Defined in

[connection.ts:93](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L93)

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

[connection.ts:173](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L173)

___

### isOpen

▸ **isOpen**(): `boolean`

Return true if the connection has not been closed

#### Returns

`boolean`

#### Defined in

[connection.ts:77](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L77)

___

### openTable

▸ **openTable**(`name`): `Promise`\<[`Table`](Table.md)\>

Open a table in the database.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `name` | `string` | The name of the table |

#### Returns

`Promise`\<[`Table`](Table.md)\>

#### Defined in

[connection.ts:112](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L112)

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

[connection.ts:104](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L104)
