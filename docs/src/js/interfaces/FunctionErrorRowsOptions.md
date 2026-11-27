[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrorRowsOptions

# Interface: FunctionErrorRowsOptions

Filters and projection for current failing inputs.

## Extends

- `Pick`&lt;[`FunctionErrorsOptions`](FunctionErrorsOptions.md), `"column"` \| `"jobId"` \| `"limit"`&gt;

## Properties

### column?

```ts
optional column: string;
```

Only errors on this column.

#### Inherited from

`Pick.column`

***

### columns?

```ts
optional columns: string[];
```

Source columns to return. Defaults to all source columns.

***

### jobId?

```ts
optional jobId: string;
```

Only errors recorded by this job.

#### Inherited from

`Pick.jobId`

***

### limit?

```ts
optional limit: number;
```

At most this many records (server default 10000, cap 100000).

#### Inherited from

`Pick.limit`
