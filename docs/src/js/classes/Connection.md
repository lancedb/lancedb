[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Connection

# Class: `abstract` Connection

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

## Constructors

### new Connection()

> **new Connection**(): [`Connection`](Connection.md)

#### Returns

[`Connection`](Connection.md)

## Methods

### close()

> `abstract` **close**(): `void`

Close the connection, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the connection after it is closed will result in an error.

#### Returns

`void`

***

### createEmptyTable()

> `abstract` **createEmptyTable**(`name`, `schema`, `options`?): `Promise`&lt;[`Table`](Table.md)&gt;

Creates a new empty Table

#### Parameters

• **name**: `string`

The name of the table.

• **schema**: `SchemaLike`

The schema of the table

• **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### createTable()

#### createTable(options)

> `abstract` **createTable**(`options`): `Promise`&lt;[`Table`](Table.md)&gt;

Creates a new Table and initialize it with new data.

##### Parameters

• **options**: `object` & `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;

The options object.

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

#### createTable(name, data, options)

> `abstract` **createTable**(`name`, `data`, `options`?): `Promise`&lt;[`Table`](Table.md)&gt;

Creates a new Table and initialize it with new data.

##### Parameters

• **name**: `string`

The name of the table.

• **data**: `TableLike` \| `Record`&lt;`string`, `unknown`&gt;[]

Non-empty Array of Records
to be inserted into the table

• **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### display()

> `abstract` **display**(): `string`

Return a brief description of the connection

#### Returns

`string`

***

### dropTable()

> `abstract` **dropTable**(`name`): `Promise`&lt;`void`&gt;

Drop an existing table.

#### Parameters

• **name**: `string`

The name of the table to drop.

#### Returns

`Promise`&lt;`void`&gt;

***

### isOpen()

> `abstract` **isOpen**(): `boolean`

Return true if the connection has not been closed

#### Returns

`boolean`

***

### openTable()

> `abstract` **openTable**(`name`, `options`?): `Promise`&lt;[`Table`](Table.md)&gt;

Open a table in the database.

#### Parameters

• **name**: `string`

The name of the table

• **options?**: `Partial`&lt;`OpenTableOptions`&gt;

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### tableNames()

> `abstract` **tableNames**(`options`?): `Promise`&lt;`string`[]&gt;

List all the table names in this database.

Tables will be returned in lexicographical order.

#### Parameters

• **options?**: `Partial`&lt;[`TableNamesOptions`](../interfaces/TableNamesOptions.md)&gt;

options to control the
paging / start point

#### Returns

`Promise`&lt;`string`[]&gt;
