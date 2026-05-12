[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / DropNamespaceOptions

# Interface: DropNamespaceOptions

## Properties

### behavior?

```ts
optional behavior: "restrict" | "cascade";
```

Refuse to drop if non-empty (restrict) or drop recursively (cascade).

***

### mode?

```ts
optional mode: "fail" | "skip";
```

Whether to skip if the namespace doesn't exist, or fail.
