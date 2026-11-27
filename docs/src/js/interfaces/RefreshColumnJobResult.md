[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RefreshColumnJobResult

# Interface: RefreshColumnJobResult

The terminal outcome of a Function retry or upgrade job.

## Properties

### errorGroups

```ts
errorGroups: FunctionErrorGroup[];
```

Grouped causes reported by the completed job.

***

### errorGroupsTruncated

```ts
errorGroupsTruncated: boolean;
```

Whether the reported cause counts omit any failures.

***

### publishedVersion?

```ts
optional publishedVersion: bigint;
```

The table version made visible by the job, if it published one.

***

### rowsAssigned

```ts
rowsAssigned: bigint;
```

Logical rows successfully assigned a value.

***

### rowsFailed

```ts
rowsFailed: bigint;
```

Logical rows whose computation failed.

***

### rowsRemaining

```ts
rowsRemaining: bigint;
```

Logical rows still needing a value, excluding rows counted in `rowsFailed`.

***

### sourceVersion

```ts
sourceVersion: bigint;
```

The exact table version read by the job.
