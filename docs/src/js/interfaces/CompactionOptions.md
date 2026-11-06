[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CompactionOptions

# Interface: CompactionOptions

Per-invocation compaction settings. Values must be safe integers.

## Properties

### batchSize?

```ts
optional batchSize: number;
```

Positive scanner batch size in rows.

***

### ioBufferSize?

```ts
optional ioBufferSize: number;
```

Scanner I/O buffer size in bytes; zero disables buffering.

***

### maxSourceRows?

```ts
optional maxSourceRows: number;
```

Positive live-source-row budget across whole tasks; an oversized first task can produce no work.

***

### targetRowsPerFragment?

```ts
optional targetRowsPerFragment: number;
```

Positive target rows per output fragment; also affects candidate selection.
