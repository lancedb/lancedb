[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / connectCatalog

# Function: connectCatalog()

```ts
function connectCatalog(endpoint, options): Promise<Catalog>
```

Connect to an HTTP(S) catalog endpoint. Catalog requests omit database-selection
headers; opened database connections inherit authentication and client options.

## Parameters

* **endpoint**: `string`

* **options**: [`CatalogOptions`](../interfaces/CatalogOptions.md) = `{}`

## Returns

`Promise`&lt;[`Catalog`](../classes/Catalog.md)&gt;

## Example

```ts
const catalog = await connectCatalog("https://my-server.example", { apiKey: "secret" });
const db = await catalog.createDatabase("analytics", { existOk: true });
const page = await catalog.listDatabases({ limit: 20 });
```
