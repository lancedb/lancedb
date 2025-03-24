[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / QueryExecutionOptions

# Interface: QueryExecutionOptions

Options that control the behavior of a particular query execution

## Properties

### maxBatchLength?

```ts
optional maxBatchLength: number;
```

The maximum number of rows to return in a single batch

Batches may have fewer rows if the underlying data is stored
in smaller chunks.
