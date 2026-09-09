[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableShardStats

# Interface: TableShardStats

Live state of one table shard. A table is N table shards on one node; flattening to a
single number hides the one hot table shard that is usually why someone opened
this endpoint.

## Properties

### compacting

```ts
compacting: boolean;
```

Whether a pass owns this table shard's compaction latch right now. Says *a*
driver is running, not *whose*, and the latch is held from dispatch —
including while the pass queues for a pod-wide compactor permit. Read it
as "do not pile on", never as "mine is progressing".

***

### currentGeneration

```ts
currentGeneration: number;
```

The generation the active memtable will become.

***

### sstables

```ts
sstables: SsTableStats[];
```

SSTables not yet merged into the base table.

***

### manifestVersion

```ts
manifestVersion: number;
```

Version of the shard manifest these numbers were read from.

***

### memtables?

```ts
optional memtables: MemtableStats[];
```

Oldest first, active last. Absent for a `"Sealed"` table shard, whose
in-memory state is torn down.

***

### replayAfterWalEntryPosition

```ts
replayAfterWalEntryPosition: number;
```

WAL position replay resumes from.

***

### shardId

```ts
shardId: string;
```

The shard this table shard writes.

***

### status

```ts
status: string;
```

`"Active"` or `"Sealed"` (drop-table 2PC in flight).

***

### walEntryPositionLastSeen

```ts
walEntryPositionLastSeen: number;
```

Highest WAL position the writer has seen. The difference against
`replayAfterWalEntryPosition` is the WAL lag.

***

### writerEpoch

```ts
writerEpoch: number;
```

Epoch of the writer that currently owns the shard.
