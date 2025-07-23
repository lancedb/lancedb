[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / connect

# Function: connect()

## connect(uri, options, session)

```ts
function connect(
   uri,
   options?,
   session?): Promise<Connection>
```

Connect to a LanceDB instance at the given URI.

Accepted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

### Parameters

* **uri**: `string`
    The uri of the database. If the database uri starts
    with `db://` then it connects to a remote database.

* **options?**: `Partial`&lt;[`ConnectionOptions`](../interfaces/ConnectionOptions.md)&gt;
    The options to use when connecting to the database

* **session?**: [`Session`](../classes/Session.md)

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

## connect(options)

```ts
function connect(options): Promise<Connection>
```

Connect to a LanceDB instance at the given URI.

Accepted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

### Parameters

* **options**: `Partial`&lt;[`ConnectionOptions`](../interfaces/ConnectionOptions.md)&gt; & `object`
    The options to use when connecting to the database

### Returns

`Promise`&lt;[`Connection`](../classes/Connection.md)&gt;

### See

[ConnectionOptions](../interfaces/ConnectionOptions.md) for more details on the URI format.

### Examples

```ts
const conn = await connect({
  uri: "/path/to/database",
  storageOptions: {timeout: "60s"}
});
```

```ts
const session = Session.default();
const conn = await connect({
  uri: "/path/to/database",
  session: session
});
```
