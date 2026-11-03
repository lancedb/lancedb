[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ConnectionOptions

# Interface: ConnectionOptions

## Properties

### apiKey?

```ts
optional apiKey: string;
```

(For LanceDB Enterprise or LanceDB Cloud only): the API key to use.

Can also be set via the environment variable `LANCEDB_API_KEY`.

***

### clientConfig?

```ts
optional clientConfig: ClientConfig;
```

(For LanceDB Enterprise or LanceDB Cloud only): configuration for the HTTP client.

***

### hostOverride?

```ts
optional hostOverride: string;
```

(For LanceDB Enterprise only): the host of the LanceDB Enterprise
deployment to connect to.

***

### manifestEnabled?

```ts
optional manifestEnabled: boolean;
```

(For LanceDB Embedded only): use directory namespace manifests as the source
of truth for table metadata. Existing directory-listed root tables are
migrated into the manifest on access.

***

### namespaceClientProperties?

```ts
optional namespaceClientProperties: Record<string, string>;
```

(For LanceDB Embedded only): extra properties for the backing namespace
client used by manifest-enabled native connections.

***

### oauthConfig?

```ts
optional oauthConfig: NativeOAuthConfig;
```

(For LanceDB Enterprise only): OAuth configuration for IdP-based
authentication (e.g., Azure Entra ID). When set, token acquisition
and refresh are handled entirely in Rust. TypeScript users should pass
the public `OAuthConfig` type exported from `@lancedb/lancedb`.

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
eventual consistency. If more than that interval has passed since the
last check, then the table will be checked for updates. Note: this
consistency only applies to read operations. Write operations are
always consistent.

Stronger consistency is not free. The smaller the interval, the more
often each read pays the cost of checking for updates against object
storage, raising per-read latency and cost.

***

### region?

```ts
optional region: string;
```

(For LanceDB Cloud only): the region to use for LanceDB Cloud (legacy).
Defaults to 'us-east-1'.

***

### session?

```ts
optional session: Session;
```

(For LanceDB Embedded only): the session to use for this connection. Holds
shared caches and other session-specific state.

***

### storageOptions?

```ts
optional storageOptions: Record<string, string>;
```

(For LanceDB Embedded only): configuration for object storage.

The available options are described at https://docs.lancedb.com/storage/
