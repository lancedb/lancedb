[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / DeltaVersion

# Type Alias: DeltaVersion

```ts
type DeltaVersion: number | bigint | string;
```

One endpoint of a table delta: a positive version number/bigint or a tag on
the table handle's current branch.

Use a `bigint` when the version may exceed `Number.MAX_SAFE_INTEGER`.
Strings are always tags.
