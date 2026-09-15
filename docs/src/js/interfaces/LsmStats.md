[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LsmStats

# Interface: LsmStats

Live per-bucket LSM state, as returned by `Table#getLsmStats`.

Nothing here is derived: sums and differences (total L0 bytes, WAL lag) are
the caller's to compute.

## Properties

### buckets

```ts
buckets: BucketStats[];
```

One entry per bucket backing this table.
