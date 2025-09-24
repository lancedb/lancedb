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

## Methods

### cloneTable()

```ts
abstract cloneTable(
   targetTableName,
   sourceUri,
   options?): Promise<Table>
```

Clone a table from a source table.

A shallow clone creates a new table that shares the underlying data files
with the source table but has its own independent manifest. This allows
both the source and cloned tables to evolve independently while initially
sharing the same data, deletion, and index files.

#### Parameters

* **targetTableName**: `string`
    The name of the target table to create.

* **sourceUri**: `string`
    The URI of the source table to clone from.

* **options?**
    Clone options.

* **options.isShallow?**: `boolean`
    Whether to perform a shallow clone (defaults to true).

* **options.sourceTag?**: `string`
    The tag of the source table to clone.

* **options.sourceVersion?**: `number`
    The version of the source table to clone.

* **options.targetNamespace?**: `string`[]
    The namespace for the target table (defaults to root namespace).

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### close()

```ts
abstract close(): void
```

Close the connection, releasing any underlying resources.

It is safe to call this method multiple times.

Any attempt to use the connection after it is closed will result in an error.

#### Returns

`void`

***

### createEmptyTable()

#### createEmptyTable(name, schema, options)

```ts
abstract createEmptyTable(
   name,
   schema,
   options?): Promise<Table>
```

Creates a new empty Table

##### Parameters

* **name**: `string`
    The name of the table.

* **schema**: [`SchemaLike`](../type-aliases/SchemaLike.md)
    The schema of the table

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options (backwards compatibility)

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

#### createEmptyTable(name, schema, namespace, options)

```ts
abstract createEmptyTable(
   name,
   schema,
   namespace?,
   options?): Promise<Table>
```

Creates a new empty Table

##### Parameters

* **name**: `string`
    The name of the table.

* **schema**: [`SchemaLike`](../type-aliases/SchemaLike.md)
    The schema of the table

* **namespace?**: `string`[]
    The namespace to create the table in (defaults to root namespace)

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### createTable()

#### createTable(options, namespace)

```ts
abstract createTable(options, namespace?): Promise<Table>
```

Creates a new Table and initialize it with new data.

##### Parameters

* **options**: `object` & `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    The options object.

* **namespace?**: `string`[]
    The namespace to create the table in (defaults to root namespace)

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

#### createTable(name, data, options)

```ts
abstract createTable(
   name,
   data,
   options?): Promise<Table>
```

Creates a new Table and initialize it with new data.

##### Parameters

* **name**: `string`
    The name of the table.

* **data**: [`TableLike`](../type-aliases/TableLike.md) \| `Record`&lt;`string`, `unknown`&gt;[]
    Non-empty Array of Records
    to be inserted into the table

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options (backwards compatibility)

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

#### createTable(name, data, namespace, options)

```ts
abstract createTable(
   name,
   data,
   namespace?,
   options?): Promise<Table>
```

Creates a new Table and initialize it with new data.

##### Parameters

* **name**: `string`
    The name of the table.

* **data**: [`TableLike`](../type-aliases/TableLike.md) \| `Record`&lt;`string`, `unknown`&gt;[]
    Non-empty Array of Records
    to be inserted into the table

* **namespace?**: `string`[]
    The namespace to create the table in (defaults to root namespace)

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### display()

```ts
abstract display(): string
```

Return a brief description of the connection

#### Returns

`string`

***

### dropAllTables()

```ts
abstract dropAllTables(namespace?): Promise<void>
```

Drop all tables in the database.

#### Parameters

* **namespace?**: `string`[]
    The namespace to drop tables from (defaults to root namespace).

#### Returns

`Promise`&lt;`void`&gt;

***

### dropTable()

```ts
abstract dropTable(name, namespace?): Promise<void>
```

Drop an existing table.

#### Parameters

* **name**: `string`
    The name of the table to drop.

* **namespace?**: `string`[]
    The namespace of the table (defaults to root namespace).

#### Returns

`Promise`&lt;`void`&gt;

***

### isOpen()

```ts
abstract isOpen(): boolean
```

Return true if the connection has not been closed

#### Returns

`boolean`

***

### openTable()

```ts
abstract openTable(
   name,
   namespace?,
   options?): Promise<Table>
```

Open a table in the database.

#### Parameters

* **name**: `string`
    The name of the table

* **namespace?**: `string`[]
    The namespace of the table (defaults to root namespace)

* **options?**: `Partial`&lt;[`OpenTableOptions`](../interfaces/OpenTableOptions.md)&gt;
    Additional options

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### tableNames()

#### tableNames(options)

```ts
abstract tableNames(options?): Promise<string[]>
```

List all the table names in this database.

Tables will be returned in lexicographical order.

##### Parameters

* **options?**: `Partial`&lt;[`TableNamesOptions`](../interfaces/TableNamesOptions.md)&gt;
    options to control the
    paging / start point (backwards compatibility)

##### Returns

`Promise`&lt;`string`[]&gt;

#### tableNames(namespace, options)

```ts
abstract tableNames(namespace?, options?): Promise<string[]>
```

List all the table names in this database.

Tables will be returned in lexicographical order.

##### Parameters

* **namespace?**: `string`[]
    The namespace to list tables from (defaults to root namespace)

* **options?**: `Partial`&lt;[`TableNamesOptions`](../interfaces/TableNamesOptions.md)&gt;
    options to control the
    paging / start point

##### Returns

`Promise`&lt;`string`[]&gt;
