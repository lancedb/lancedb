[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / SsTableStats

# Interface: SsTableStats

One SSTable.

## Properties

### bytes

```ts
bytes: number;
```

On-disk size of the SSTable.

***

### generation

```ts
generation: number;
```

The generation number. Increases as memtables are frozen into SSTables.

***

### rows?

```ts
optional rows: number;
```

Present only when `includeSstableRows` was requested. Off by default
because each count opens an uncached Lance dataset.
