[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OptimizeResolvedOptions

# Interface: OptimizeResolvedOptions

Effective settings after Enterprise policy resolution.

## Properties

### batchSize

```ts
batchSize: null | number;
```

***

### ioBufferSize

```ts
ioBufferSize: null | number;
```

Zero disables byte-based I/O backpressure; null uses the worker default.

***

### maxSourceRows

```ts
maxSourceRows: null | number;
```

***

### targetRowsPerFragment

```ts
targetRowsPerFragment: number;
```
