[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / connect

# Function: connect()

## connect(uri, opts)

> **connect**(`uri`, `opts`?): `Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

Connect to a LanceDB instance at the given URI.

Accepted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

### Parameters

• **uri**: `string`

The uri of the database. If the database uri starts
with `db://` then it connects to a remote database.

• **opts?**: `Partial`&lt;[`ConnectionOptions`](../interfaces/ConnectionOptions.md) \| `RemoteConnectionOptions`&gt;

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

### See

[ConnectionOptions](../interfaces/ConnectionOptions.md) for more details on the URI format.

### Examples

```ts
const conn = await connect("/path/to/database");
```

```ts
const conn = await connect(
  "s3://bucket/path/to/database",
  {storageOptions: {timeout: "60s"}
});
```

## connect(opts)

> **connect**(`opts`): `Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

Connect to a LanceDB instance at the given URI.

Accepted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

### Parameters

• **opts**: `Partial`&lt;[`ConnectionOptions`](../interfaces/ConnectionOptions.md) \| `RemoteConnectionOptions`&gt; & `object`

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

### See

[ConnectionOptions](../interfaces/ConnectionOptions.md) for more details on the URI format.

### Example

```ts
const conn = await connect({
  uri: "/path/to/database",
  storageOptions: {timeout: "60s"}
});
```
