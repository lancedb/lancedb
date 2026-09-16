[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Catalog

# Class: Catalog

A remote catalog manages databases through the server's root namespace.

## Accessors

### uri

```ts
get uri(): string
```

The root namespace endpoint.

#### Returns

`string`

## Methods

### createDatabase()

```ts
createDatabase(name, options): Promise<Connection>
```

Create a database, or open an existing database when existOk is true.

#### Parameters

* **name**: `string`

* **options** = `{}`

* **options.existOk?**: `boolean`

#### Returns

`Promise`&lt;[`Connection`](Connection.md)&gt;

***

### dropDatabase()

```ts
dropDatabase(name, options): Promise<void>
```

Drop an empty database. The server rejects nonempty databases.

#### Parameters

* **name**: `string`

* **options** = `{}`

* **options.ignoreMissing?**: `boolean`

#### Returns

`Promise`&lt;`void`&gt;

***

### listDatabases()

```ts
listDatabases(options): Promise<ListDatabasesResponse>
```

List one page of databases; pass pageToken from a response for the next page.

#### Parameters

* **options** = `{}`

* **options.limit?**: `number`

* **options.pageToken?**: `string`

#### Returns

`Promise`&lt;[`ListDatabasesResponse`](../interfaces/ListDatabasesResponse.md)&gt;

***

### openDatabase()

```ts
openDatabase(name): Promise<Connection>
```

Open an existing database by its logical name.

#### Parameters

* **name**: `string`

#### Returns

`Promise`&lt;[`Connection`](Connection.md)&gt;
