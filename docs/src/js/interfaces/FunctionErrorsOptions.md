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

### history?

```ts
optional history: boolean;
```

Include retained historical attempts, including repaired rows. Requires workflow version 1 or later.

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

***

### pageToken?

```ts
optional pageToken: string;
```

Empty starts retained history, including repaired rows, for an exact jobId; otherwise continue that page.
