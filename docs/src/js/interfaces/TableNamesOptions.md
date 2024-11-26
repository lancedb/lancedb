[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../README.md) / TableNamesOptions

# Interface: TableNamesOptions

## Properties

### limit?

```ts
optional limit: number;
```

An optional limit to the number of results to return.

***

### startAfter?

```ts
optional startAfter: string;
```

If present, only return names that come lexicographically after the
supplied value.

This can be combined with limit to implement pagination by setting this to
the last table name from the previous page.
