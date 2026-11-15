[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CompactionResult

# Interface: CompactionResult

Actual committed outcome of one remote compaction job.

## Properties

### completionPercentage

```ts
completionPercentage: number;
```

Percentage of planned live rows committed, from 0 to 100.

***

### filesAdded

```ts
filesAdded: null | number;
```

Data files added by committed rewrites; null if an older worker omitted it.

***

### filesRemoved

```ts
filesRemoved: null | number;
```

Data files removed by committed rewrites; null if an older worker omitted it.

***

### fragmentsAdded

```ts
fragmentsAdded: null | number;
```

Fragments added by committed rewrites; null if an older worker omitted it.

***

### fragmentsRemoved

```ts
fragmentsRemoved: null | number;
```

Fragments removed by committed rewrites; null if an older worker omitted it.

***

### moreWork

```ts
moreWork: boolean;
```

A bounded run left eligible work for another invocation.

***

### partialCommit

```ts
partialCommit: boolean;
```

Some planned groups were not committed.

***

### rowsFailed

```ts
rowsFailed: number;
```

Planned live rows not committed by this job.

***

### rowsSucceeded

```ts
rowsSucceeded: number;
```

Live source rows in committed rewrite groups.
