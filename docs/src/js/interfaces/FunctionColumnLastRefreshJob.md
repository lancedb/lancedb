[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionColumnLastRefreshJob

# Interface: FunctionColumnLastRefreshJob

Latest matching refresh Job projected for a Function output.

## Properties

### automatic?

```ts
optional automatic: boolean;
```

***

### completedAtMillis?

```ts
optional completedAtMillis: bigint;
```

***

### failure?

```ts
optional failure: JobFailureInfo;
```

Structured failure details for a failed Job.

***

### jobId

```ts
jobId: string;
```

***

### state

```ts
state: string;
```
