[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../README.md) / TimeoutConfig

# Interface: TimeoutConfig

Timeout configuration for remote HTTP client.

## Properties

### connectTimeout?

```ts
optional connectTimeout: number;
```

The timeout for establishing a connection in seconds. Default is 120
seconds (2 minutes). This can also be set via the environment variable
`LANCE_CLIENT_CONNECT_TIMEOUT`, as an integer number of seconds.

***

### poolIdleTimeout?

```ts
optional poolIdleTimeout: number;
```

The timeout for keeping idle connections in the connection pool in seconds.
Default is 300 seconds (5 minutes). This can also be set via the
environment variable `LANCE_CLIENT_CONNECTION_TIMEOUT`, as an integer
number of seconds.

***

### readTimeout?

```ts
optional readTimeout: number;
```

The timeout for reading data from the server in seconds. Default is 300
seconds (5 minutes). This can also be set via the environment variable
`LANCE_CLIENT_READ_TIMEOUT`, as an integer number of seconds.
