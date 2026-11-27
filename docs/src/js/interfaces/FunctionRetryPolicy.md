[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionRetryPolicy

# Interface: FunctionRetryPolicy

Overrides retries of failed user-code calls for one refresh. Omitted
fields use the Function refresh defaults.

## Properties

### errorCodes?

```ts
optional errorCodes: string[];
```

Structured provider error codes or HTTP status codes.

***

### exceptionTypes?

```ts
optional exceptionTypes: string[];
```

Exception class names, preferably qualified.

***

### honorRetryAfter?

```ts
optional honorRetryAfter: boolean;
```

Honor Retry-After within the configured maximum.

***

### initialBackoffSeconds?

```ts
optional initialBackoffSeconds: number;
```

Initial exponential-backoff ceiling, in seconds.

***

### maxAttempts?

```ts
optional maxAttempts: number;
```

Total calls, including the first; 1 disables retries, maximum 100.

***

### maxBackoffSeconds?

```ts
optional maxBackoffSeconds: number;
```

Maximum backoff, in seconds.

***

### messagePattern?

```ts
optional messagePattern: string;
```

Regular expression matched against the exception class and message.

***

### transient?

```ts
optional transient: boolean;
```

Match connection/timeouts, HTTP 429/503, SlowDown and
ServiceUnavailable.
