[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / GenerationStats

# Interface: GenerationStats

One flushed L0 generation.

## Properties

### bytes

```ts
bytes: number;
```

On-disk size of the generation.

***

### generation

```ts
generation: number;
```

The generation number. Increases as memtables are sealed into L0.

***

### rows?

```ts
optional rows: number;
```

Present only when `includeGenerationRows` was requested. Off by default
because each count opens an uncached Lance dataset.
