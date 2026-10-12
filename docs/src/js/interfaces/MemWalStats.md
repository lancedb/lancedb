[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MemWalStats

# Interface: MemWalStats

Live per-table-shard MemWAL state, as returned by `Table#getMemWalStats`.

Nothing here is derived: sums and differences (total SSTable bytes, WAL lag) are
the caller's to compute.

## Properties

### tableShards

```ts
tableShards: TableShardStats[];
```

One entry per table shard backing this table.
