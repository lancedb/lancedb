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

### connectDatabase()

```ts
connectDatabase(name): Promise<Connection>
```

Connect to an existing database by its logical name.

#### Parameters

* **name**: `string`

#### Returns

`Promise`&lt;[`Connection`](Connection.md)&gt;

***

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

Drop a database. A nonempty database is an error unless `cascade` is set,
which drops everything in it first; the name stays taken until the server
finishes cleaning up their storage.

#### Parameters

* **name**: `string`

* **options** = `{}`

* **options.cascade?**: `boolean`

* **options.ignoreMissing?**: `boolean`

#### Returns

`Promise`&lt;`void`&gt;

***

### listDatabases()

```ts
listDatabases(options): DatabaseNames
```

Iterate lazily over all database names using `for await...of`.
pageToken resumes from a saved token; omitted starts at the beginning.
pageLimit limits each REST response, not the total; omitted uses the server default.
Request errors are raised during iteration and terminate the iterator.

#### Parameters

* **options** = `{}`

* **options.pageLimit?**: `number`

* **options.pageToken?**: `string`

#### Returns

[`DatabaseNames`](DatabaseNames.md)

#### Example

```ts
for await (const name of catalog.listDatabases({ pageLimit: 20 })) {
  console.log(name);
}
```
