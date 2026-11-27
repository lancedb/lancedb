[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobFailureInfo

# Interface: JobFailureInfo

The server's account of why a job failed.

## Properties

### message?

```ts
optional message: string;
```

***

### phase?

```ts
optional phase: string;
```

***

### retryable?

```ts
optional retryable: boolean;
```

***

### sqlJobId?

```ts
optional sqlJobId: string;
```

Isolated SQL job that raised this failure, when supplied by the server.

***

### traceback?

```ts
optional traceback: string;
```

User-code traceback, possibly truncated by the server.
