[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ConnectionOptions

# Interface: ConnectionOptions

## Properties

### apiKey?

```ts
optional apiKey: string;
```

(For LanceDB cloud only): the API key to use with LanceDB Cloud.

Can also be set via the environment variable `LANCEDB_API_KEY`.

***

### clientConfig?

```ts
optional clientConfig: ClientConfig;
```

(For LanceDB cloud only): configuration for the remote HTTP client.

***

### hostOverride?

```ts
optional hostOverride: string;
```

(For LanceDB cloud only): the host to use for LanceDB cloud. Used
for testing purposes.

***

### readConsistencyInterval?

```ts
optional readConsistencyInterval: number;
```

(For LanceDB OSS only): The interval, in seconds, at which to check for
updates to the table from other processes. If None, then consistency is not
checked. For performance reasons, this is the default. For strong
consistency, set this to zero seconds. Then every read will check for
updates from other processes. As a compromise, you can set this to a
non-zero value for eventual consistency. If more than that interval
has passed since the last check, then the table will be checked for updates.
Note: this consistency only applies to read operations. Write operations are
always consistent.

***

### region?

```ts
optional region: string;
```

(For LanceDB cloud only): the region to use for LanceDB cloud.
Defaults to 'us-east-1'.

***

### session?

```ts
optional session: Session;
```

(For LanceDB OSS only): the session to use for this connection. Holds
shared caches and other session-specific state.

***

### storageOptions?

```ts
optional storageOptions: Record<string, string>;
```

(For LanceDB OSS only): configuration for object storage.

The available options are described at https://lancedb.com/docs/storage/integrations/
