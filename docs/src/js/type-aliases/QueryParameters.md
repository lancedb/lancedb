[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / QueryParameters

# Type Alias: QueryParameters

```ts
type QueryParameters: readonly QueryParameterValue[] | Readonly<Record<string, QueryParameterValue>> | TableLike | RecordBatchLike;
```

Values for `$1`/`$name` placeholders, sent separately from SQL text.
Arrays bind by position; objects bind by name. Arrow columns bind both ways.
Safe integers and bigint use Int64; fractional numbers use Float64.
Arrow input preserves explicit types.
