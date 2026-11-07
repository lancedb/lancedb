[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LanceErrorOptions

# Interface: LanceErrorOptions

## Extends

- `ErrorOptions`

## Properties

### cause?

```ts
optional cause: unknown;
```

#### Inherited from

`ErrorOptions.cause`

***

### code?

```ts
optional code: string;
```

Rust variant name. Subclasses set this from their static [LanceError.code](../classes/LanceError.md#code-1).

***

### columnName?

```ts
optional columnName: string;
```

***

### connectFailures?

```ts
optional connectFailures: number;
```

***

### databaseName?

```ts
optional databaseName: string;
```

***

### errorCode?

```ts
optional errorCode: string;
```

Machine-readable code reported by a remote server, when it sent one.

***

### functionName?

```ts
optional functionName: string;
```

***

### grpcCode?

```ts
optional grpcCode: number;
```

***

### indexName?

```ts
optional indexName: string;
```

***

### jobId?

```ts
optional jobId: string;
```

***

### jobRequestId?

```ts
optional jobRequestId: string;
```

***

### maxConnectFailures?

```ts
optional maxConnectFailures: number;
```

***

### maxReadFailures?

```ts
optional maxReadFailures: number;
```

***

### maxRequestFailures?

```ts
optional maxRequestFailures: number;
```

***

### namespacePath?

```ts
optional namespacePath: string[];
```

***

### path?

```ts
optional path: string;
```

***

### phase?

```ts
optional phase: string;
```

***

### readFailures?

```ts
optional readFailures: number;
```

***

### requestFailures?

```ts
optional requestFailures: number;
```

***

### requestId?

```ts
optional requestId: string;
```

***

### resourceKind?

```ts
optional resourceKind: string;
```

***

### resourceName?

```ts
optional resourceName: string;
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

***

### statusCode?

```ts
optional statusCode: number;
```

***

### tableName?

```ts
optional tableName: string;
```
