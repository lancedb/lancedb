[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MaterializedViewSelect

# Type Alias: MaterializedViewSelect

```ts
type MaterializedViewSelect: (string | [string, string])[] | Record<string, string>;
```

The view's columns: column names, `[alias, SQL expression]` pairs, or a
record of the same. A bare name projects itself.
