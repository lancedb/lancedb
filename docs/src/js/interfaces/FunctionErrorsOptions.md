[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrorsOptions

# Interface: FunctionErrorsOptions

Which per-row Function errors to list; every filter is optional.

## Properties

### column?

```ts
optional column: string;
```

Only errors on this column.

***

### jobId?

```ts
optional jobId: string;
```

Only errors recorded by this job.

***

### limit?

```ts
optional limit: number;
```

At most this many records (server default 10000, cap 100000).
