[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BranchDiff

# Interface: BranchDiff

Read-only comparison of a branch against main.

## Properties

### addedColumns

```ts
addedColumns: BranchColumnSummary[];
```

***

### addedIndexes

```ts
addedIndexes: BranchIndexSummary[];
```

***

### baseMoved

```ts
baseMoved: boolean;
```

***

### branchVersion

```ts
branchVersion: number;
```

***

### changedColumns

```ts
changedColumns: BranchColumnChange[];
```

***

### errors

```ts
errors: CherryPickError[];
```

***

### fromBranch

```ts
fromBranch: string;
```

***

### mainVersion

```ts
mainVersion: number;
```

***

### parentVersion

```ts
parentVersion: number;
```

***

### removedColumns

```ts
removedColumns: BranchColumnSummary[];
```

***

### removedIndexes

```ts
removedIndexes: BranchIndexSummary[];
```

***

### rowCountBranch

```ts
rowCountBranch: number;
```

***

### rowCountMain

```ts
rowCountMain: number;
```

***

### rowSummary

```ts
rowSummary: BranchRowCountSummary;
```
