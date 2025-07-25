[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / IndexStatistics

# Interface: IndexStatistics

## Properties

### distanceType?

```ts
optional distanceType: string;
```

The type of the distance function used by the index. This is only
present for vector indices. Scalar and full text search indices do
not have a distance function.

***

### indexType

```ts
indexType: string;
```

The type of the index

***

### loss?

```ts
optional loss: number;
```

The K-means loss value of the index,
it is only present for vector indices.

***

### numIndexedRows

```ts
numIndexedRows: number;
```

The number of rows indexed by the index

***

### numIndices?

```ts
optional numIndices: number;
```

The number of parts this index is split into.

***

### numUnindexedRows

```ts
numUnindexedRows: number;
```

The number of rows not indexed
