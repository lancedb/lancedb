[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionErrorFragment

# Interface: FunctionErrorFragment

A fragment whose per-row error detail was capped: `rowsSkipped` rows
failed, of which only `rowsRecorded` have a record of their own.

## Properties

### fragmentId

```ts
fragmentId: number;
```

***

### jobId

```ts
jobId: string;
```

***

### rowsRecorded

```ts
rowsRecorded: number;
```

***

### rowsSkipped

```ts
rowsSkipped: number;
```
