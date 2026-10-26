[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RTEStats

# Interface: RTEStats

Live per-table-shard RTE state, as returned by `Table#getRTEStats`.

Nothing here is derived: sums and differences (total SSTable bytes, WAL lag) are
the caller's to compute.

## Properties

### tableShards

```ts
tableShards: TableShardStats[];
```

One entry per table shard backing this table.
