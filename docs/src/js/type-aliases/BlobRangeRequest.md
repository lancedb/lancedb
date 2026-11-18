[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BlobRangeRequest

# Type Alias: BlobRangeRequest

```ts
type BlobRangeRequest: object;
```

One row-specific range for [Table.fetchBlobRanges](../classes/Table.md#fetchblobranges).

## Type declaration

### length

```ts
length: bigint;
```

Number of bytes to read, as a uint64. Zero returns an empty non-null value.

### offset

```ts
offset: bigint;
```

Byte offset from the beginning of the logical blob, as a uint64.

### rowId

```ts
rowId: bigint | number;
```

A row ID from a query with row IDs enabled.
