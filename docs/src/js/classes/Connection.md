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

* **options.targetNamespacePath?**: `string`[]
    The namespace path for the target table (defaults to root namespace).

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

#### createEmptyTable(name, schema, namespacePath, options)

```ts
abstract createEmptyTable(
   name,
   schema,
   namespacePath?,
   options?): Promise<Table>
```

Creates a new empty Table

##### Parameters

* **name**: `string`
    The name of the table.

* **schema**: [`SchemaLike`](../type-aliases/SchemaLike.md)
    The schema of the table

* **namespacePath?**: `string`[]
    The namespace path to create the table in (defaults to root namespace)

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### createNamespace()

```ts
abstract createNamespace(namespacePath, options?): Promise<CreateNamespaceResponse>
```

Create a new namespace at the given path.

#### Parameters

* **namespacePath**: `string`[]
    The namespace path to create.

* **options?**: `Partial`&lt;[`CreateNamespaceOptions`](../interfaces/CreateNamespaceOptions.md)&gt;
    Creation `mode`
    ("create" | "exist_ok" | "overwrite") and optional `properties`
    to attach to the namespace.

#### Returns

`Promise`&lt;[`CreateNamespaceResponse`](../interfaces/CreateNamespaceResponse.md)&gt;

The properties of the
  created namespace and an optional transaction id.

***

### createTable()

#### createTable(options, namespacePath)

```ts
abstract createTable(options, namespacePath?): Promise<Table>
```

Creates a new Table and initialize it with new data.

##### Parameters

* **options**: `object` & `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    The options object.

* **namespacePath?**: `string`[]
    The namespace path to create the table in (defaults to root namespace)

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

#### createTable(name, data, namespacePath, options)

```ts
abstract createTable(
   name,
   data,
   namespacePath?,
   options?): Promise<Table>
```

Creates a new Table and initialize it with new data.

##### Parameters

* **name**: `string`
    The name of the table.

* **data**: [`TableLike`](../type-aliases/TableLike.md) \| `Record`&lt;`string`, `unknown`&gt;[]
    Non-empty Array of Records
    to be inserted into the table

* **namespacePath?**: `string`[]
    The namespace path to create the table in (defaults to root namespace)

* **options?**: `Partial`&lt;[`CreateTableOptions`](../interfaces/CreateTableOptions.md)&gt;
    Additional options

##### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### describeNamespace()

```ts
abstract describeNamespace(namespacePath): Promise<DescribeNamespaceResponse>
```

Describe a namespace, returning its properties.

#### Parameters

* **namespacePath**: `string`[]
    The namespace path to describe, in
    parent → child order, e.g. `["analytics", "sales"]`.

#### Returns

`Promise`&lt;[`DescribeNamespaceResponse`](../interfaces/DescribeNamespaceResponse.md)&gt;

The namespace's properties
  (may be undefined if the namespace has none).

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
abstract dropAllTables(namespacePath?): Promise<void>
```

Drop all tables in the database.

#### Parameters

* **namespacePath?**: `string`[]
    The namespace path to drop tables from (defaults to root namespace).

#### Returns

`Promise`&lt;`void`&gt;

***

### dropNamespace()

```ts
abstract dropNamespace(namespacePath, options?): Promise<DropNamespaceResponse>
```

Drop a namespace.

Use `behavior: "cascade"` to also drop everything contained in the
namespace (sub-namespaces and tables). The default `"restrict"`
behavior refuses to drop a non-empty namespace.

#### Parameters

* **namespacePath**: `string`[]
    The namespace path to drop.

* **options?**: `Partial`&lt;[`DropNamespaceOptions`](../interfaces/DropNamespaceOptions.md)&gt;
    `mode` ("skip" | "fail"
    for missing-namespace handling) and `behavior` ("restrict" | "cascade").

#### Returns

`Promise`&lt;[`DropNamespaceResponse`](../interfaces/DropNamespaceResponse.md)&gt;

Any properties returned by
  the server and an optional transaction id.

***

### dropTable()

```ts
abstract dropTable(name, namespacePath?): Promise<void>
```

Drop an existing table.

#### Parameters

* **name**: `string`
    The name of the table to drop.

* **namespacePath?**: `string`[]
    The namespace path of the table (defaults to root namespace).

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

### listNamespaces()

```ts
abstract listNamespaces(namespacePath?, options?): Promise<ListNamespacesResponse>
```

List the immediate child namespaces under the given parent.

Results may be paginated. To retrieve subsequent pages, pass the
`pageToken` returned by a previous call.

#### Parameters

* **namespacePath?**: `string`[]
    The parent namespace path. Defaults
    to the root namespace if omitted.

* **options?**: `Partial`&lt;[`ListNamespacesOptions`](../interfaces/ListNamespacesOptions.md)&gt;
    Pagination options
    (`pageToken`, `limit`).

#### Returns

`Promise`&lt;[`ListNamespacesResponse`](../interfaces/ListNamespacesResponse.md)&gt;

Child namespace names and
  an optional token for fetching the next page.

***

### openTable()

```ts
abstract openTable(
   name,
   namespacePath?,
   options?): Promise<Table>
```

Open a table in the database.

#### Parameters

* **name**: `string`
    The name of the table

* **namespacePath?**: `string`[]
    The namespace path of the table (defaults to root namespace)

* **options?**: `Partial`&lt;[`OpenTableOptions`](../interfaces/OpenTableOptions.md)&gt;
    Additional options

#### Returns

`Promise`&lt;[`Table`](Table.md)&gt;

***

### renameTable()

```ts
abstract renameTable(
   oldName,
   newName,
   namespacePath?): Promise<void>
```

#### Parameters

* **oldName**: `string`

* **newName**: `string`

* **namespacePath?**: `string`[]

#### Returns

`Promise`&lt;`void`&gt;

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

#### tableNames(namespacePath, options)

```ts
abstract tableNames(namespacePath?, options?): Promise<string[]>
```

List all the table names in this database.

Tables will be returned in lexicographical order.

##### Parameters

* **namespacePath?**: `string`[]
    The namespace path to list tables from (defaults to root namespace)

* **options?**: `Partial`&lt;[`TableNamesOptions`](../interfaces/TableNamesOptions.md)&gt;
    options to control the
    paging / start point

##### Returns

`Promise`&lt;`string`[]&gt;
