[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / connectNamespace

# Function: connectNamespace()

```ts
function connectNamespace(
   implName,
   properties,
   options?): Promise<Connection>
```

Connect to a LanceDB database through a namespace.

Unlike [connect](connect.md), which routes by URI scheme (local path vs.
`db://` cloud), `connectNamespace` always returns a namespace-backed
connection. The `implName` selects the namespace implementation:

- `"dir"` — directory namespace (local or object storage; configured by
  `properties.root`).
- `"rest"` — remote namespace catalog reached over HTTP (Unity, Glue,
  REST, etc.; configured via `properties`).
- A full module path for a custom implementation.

## Parameters

* **implName**: `string`
    The namespace implementation name.

* **properties**: `Record`&lt;`string`, `string`&gt;
    Configuration for the namespace implementation.

* **options?**: `Partial`&lt;[`ConnectNamespaceOptions`](../interfaces/ConnectNamespaceOptions.md)&gt;
    Optional connection settings.

## Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

## Example

```ts
const db = await connectNamespace("dir", { root: "/path/to/db" });
await db.createTable("users", [{ id: 1 }]);
```
