[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BucketStats

# Interface: BucketStats

Live state of one bucket. A table is N buckets on one node; flattening to a
single number hides the one hot bucket that is usually why someone opened
this endpoint.

## Properties

### compacting

```ts
compacting: boolean;
```

Whether a pass owns this bucket's compaction latch right now. Says *a*
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

### generations

```ts
generations: GenerationStats[];
```

Flushed L0 generations not yet merged into the base table.

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

Oldest first, active last. Absent for a `"Sealed"` bucket, whose
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

The shard this bucket writes.

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
