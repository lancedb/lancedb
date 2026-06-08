[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ScannableOptions

# Interface: ScannableOptions

## Properties

### numRows?

```ts
optional numRows: number;
```

Hint about the number of rows. Not validated against the stream.

***

### rescannable?

```ts
optional rescannable: boolean;
```

Whether the source can be scanned more than once. Defaults to `true` for
`fromTable` / `fromFactory` and `false` for `fromIterable` /
`fromRecordBatchReader`.
