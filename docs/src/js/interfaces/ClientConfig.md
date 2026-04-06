[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ClientConfig

# Interface: ClientConfig

## Properties

### extraHeaders?

```ts
optional extraHeaders: Record<string, string>;
```

***

### idDelimiter?

```ts
optional idDelimiter: string;
```

***

### retryConfig?

```ts
optional retryConfig: RetryConfig;
```

***

### timeoutConfig?

```ts
optional timeoutConfig: TimeoutConfig;
```

***

### tlsConfig?

```ts
optional tlsConfig: TlsConfig;
```

***

### userAgent?

```ts
optional userAgent: string;
```

***

### userId?

```ts
optional userId: string;
```

User identifier for tracking purposes.

This is sent as the `x-lancedb-user-id` header in requests to LanceDB Cloud/Enterprise.
It can be set directly, or via the `LANCEDB_USER_ID` environment variable.
Alternatively, set `LANCEDB_USER_ID_ENV_KEY` to specify another environment
variable that contains the user ID value.
