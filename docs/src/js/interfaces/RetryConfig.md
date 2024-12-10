[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RetryConfig

# Interface: RetryConfig

Retry configuration for the remote HTTP client.

## Properties

### backoffFactor?

```ts
optional backoffFactor: number;
```

The backoff factor to apply between retries. Default is 0.25. Between each retry
the client will wait for the amount of seconds:
`{backoff factor} * (2 ** ({number of previous retries}))`. So for the default
of 0.25, the first retry will wait 0.25 seconds, the second retry will wait 0.5
seconds, the third retry will wait 1 second, etc.

You can also set this via the environment variable
`LANCE_CLIENT_RETRY_BACKOFF_FACTOR`.

***

### backoffJitter?

```ts
optional backoffJitter: number;
```

The jitter to apply to the backoff factor, in seconds. Default is 0.25.

A random value between 0 and `backoff_jitter` will be added to the backoff
factor in seconds. So for the default of 0.25 seconds, between 0 and 250
milliseconds will be added to the sleep between each retry.

You can also set this via the environment variable
`LANCE_CLIENT_RETRY_BACKOFF_JITTER`.

***

### connectRetries?

```ts
optional connectRetries: number;
```

The maximum number of retries for connection errors. Default is 3. You
can also set this via the environment variable `LANCE_CLIENT_CONNECT_RETRIES`.

***

### readRetries?

```ts
optional readRetries: number;
```

The maximum number of retries for read errors. Default is 3. You can also
set this via the environment variable `LANCE_CLIENT_READ_RETRIES`.

***

### retries?

```ts
optional retries: number;
```

The maximum number of retries for a request. Default is 3. You can also
set this via the environment variable `LANCE_CLIENT_MAX_RETRIES`.

***

### statuses?

```ts
optional statuses: number[];
```

The HTTP status codes for which to retry the request. Default is
[429, 500, 502, 503].

You can also set this via the environment variable
`LANCE_CLIENT_RETRY_STATUSES`. Use a comma-separated list of integers.
