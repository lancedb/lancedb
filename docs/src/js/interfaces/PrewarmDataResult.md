[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PrewarmDataResult

# Interface: PrewarmDataResult

Reported executor work for a pinned snapshot; unavailable measurements stay null.

## Properties

### bytesRead

```ts
bytesRead: null | number;
```

Reported bytes read, or null when unavailable.

***

### executorsCompleted

```ts
executorsCompleted: null | number;
```

Executors that finished successfully or partially, or null when unavailable.

***

### executorsPartial

```ts
executorsPartial: null | number;
```

Subset of completed executors reporting partial warming, or null when unavailable.

***

### executorsTargeted

```ts
executorsTargeted: null | number;
```

Reported executors targeted, or null when unavailable.

***

### fragmentsSelected

```ts
fragmentsSelected: null | number;
```

Reported fragments selected, or null when unavailable.

***

### rangesCompleted

```ts
rangesCompleted: null | number;
```

Reported ranges completed, or null when unavailable.

***

### tableVersion

```ts
tableVersion: number;
```

The source snapshot captured by submission.
