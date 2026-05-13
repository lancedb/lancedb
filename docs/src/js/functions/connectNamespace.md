[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / connectNamespace

# Function: connectNamespace()

## connectNamespace(implName, config, options)

```ts
function connectNamespace(
   implName,
   config,
   options?): Promise<Connection>
```

Connect to a LanceDB database through a namespace.

Unlike [connect](connect.md), which routes by URI scheme (local path vs.
`db://` cloud), `connectNamespace` always returns a namespace-backed
connection. The `implName` selects the namespace implementation:

- `"dir"` — directory namespace, configured with [DirNamespaceConfig](../interfaces/DirNamespaceConfig.md).
- `"rest"` — remote REST catalog, configured with [RestNamespaceConfig](../interfaces/RestNamespaceConfig.md).
- Any other string — full module path for a custom implementation,
  configured with a free-form string-keyed `properties` map.

### Parameters

* **implName**: `"dir"`

* **config**: [`DirNamespaceConfig`](../interfaces/DirNamespaceConfig.md)

* **options?**: `Partial`&lt;[`ConnectNamespaceOptions`](../interfaces/ConnectNamespaceOptions.md)&gt;

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

### Examples

```ts
const db = await connectNamespace("dir", { root: "/path/to/db" });
await db.createTable("users", [{ id: 1 }]);
```

```ts
const db = await connectNamespace("rest", {
  uri: "https://catalog.example.com",
  headers: { "x-api-key": process.env.CATALOG_KEY ?? "" },
});
```

```ts
const db = await connectNamespace("my.custom.Namespace", {
  endpoint: "...",
});
```

## connectNamespace(implName, config, options)

```ts
function connectNamespace(
   implName,
   config,
   options?): Promise<Connection>
```

### Parameters

* **implName**: `"rest"`

* **config**: [`RestNamespaceConfig`](../interfaces/RestNamespaceConfig.md)

* **options?**: `Partial`&lt;[`ConnectNamespaceOptions`](../interfaces/ConnectNamespaceOptions.md)&gt;

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

## connectNamespace(implName, properties, options)

```ts
function connectNamespace(
   implName,
   properties,
   options?): Promise<Connection>
```

### Parameters

* **implName**: `string`

* **properties**: `Record`&lt;`string`, `string`&gt;

* **options?**: `Partial`&lt;[`ConnectNamespaceOptions`](../interfaces/ConnectNamespaceOptions.md)&gt;

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;
