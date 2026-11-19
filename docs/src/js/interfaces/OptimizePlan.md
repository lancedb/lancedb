[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OptimizePlan

# Interface: OptimizePlan

Data-rewrite preview without creating a maintenance job.
Counts describe planned work, not committed results, runtime, or memory use.
A later submission chooses its own snapshot and may select different work.

## Properties

### branch

```ts
branch: string;
```

***

### effectiveOptions

```ts
effectiveOptions: OptimizeResolvedOptions;
```

***

### moreWork

```ts
moreWork: boolean;
```

***

### namespace

```ts
namespace: string[];
```

***

### operation

```ts
operation: "rewrite_data";
```

***

### plannedFragments

```ts
plannedFragments: number;
```

***

### plannedGroups

```ts
plannedGroups: number;
```

***

### plannedSourceRows

```ts
plannedSourceRows: number;
```

***

### sourceVersion

```ts
sourceVersion: number;
```

***

### table

```ts
table: string;
```
