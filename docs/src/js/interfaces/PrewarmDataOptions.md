[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PrewarmDataOptions

# Interface: PrewarmDataOptions

Settings for data-cache prewarm submissions and metadata previews.

## Properties

### columns?

```ts
optional columns: string[];
```

Lance field paths to warm; omitted or empty means every column.

***

### concurrency?

```ts
optional concurrency: number;
```

Positive per-executor concurrency, at most 4294967295.

***

### filter?

```ts
optional filter: string;
```

A single SQL predicate for conservative metadata pruning.

***

### parameters?

```ts
optional parameters: QueryParameters;
```

Typed positional or named filter values, sent separately from SQL.
