[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LsmStats

# Interface: LsmStats

Live per-table-shard LSM state, as returned by `Table#getLsmStats`.

Nothing here is derived: sums and differences (total SSTable bytes, WAL lag) are
the caller's to compute.

## Properties

### tableShards

```ts
tableShards: TableShardStats[];
```

One entry per table shard backing this table.
