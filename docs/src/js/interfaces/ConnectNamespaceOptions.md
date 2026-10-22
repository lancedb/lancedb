[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ConnectNamespaceOptions

# Interface: ConnectNamespaceOptions

## Properties

### namespaceClientProperties?

```ts
optional namespaceClientProperties: Record<string, string>;
```

Extra properties for the backing namespace client.

***

### readConsistencyInterval?

```ts
optional readConsistencyInterval: number;
```

The interval, in seconds, at which to check for updates to the table
from other processes. If None, then consistency is not checked. For
performance reasons, this is the default. For strong consistency, set
this to zero seconds. Then every read will check for updates from other
processes. As a compromise, you can set this to a non-zero value for
eventual consistency.

***

### session?

```ts
optional session: Session;
```

The session to use for this connection. Holds shared caches and other
session-specific state.

***

### storageOptions?

```ts
optional storageOptions: Record<string, string>;
```

Configuration for object storage. The available options are described
at https://docs.lancedb.com/storage/
