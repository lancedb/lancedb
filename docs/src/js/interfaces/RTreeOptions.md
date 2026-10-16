[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RTreeOptions

# Interface: RTreeOptions

Options to create an `RTREE` index

## Properties

### pageSize?

```ts
optional pageSize: number;
```

The maximum number of entries in each R-tree page.

Must be at least 2. The default is 4096.
