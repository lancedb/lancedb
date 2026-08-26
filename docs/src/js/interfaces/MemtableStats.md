[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MemtableStats

# Interface: MemtableStats

One in-memory memtable.

## Properties

### batches

```ts
batches: number;
```

Record batches currently buffered.

***

### bytes

```ts
bytes: number;
```

Estimated in-memory size.

***

### generation

```ts
generation: number;
```

The generation this memtable will become once sealed.

***

### indexes

```ts
indexes: string[];
```

Names of the indexes this memtable carries. An absent name is the whole
answer to "why is my fresh-tier search on that column brute-force".

***

### rows

```ts
rows: number;
```

Rows currently buffered.
