[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CleanupResult

# Interface: CleanupResult

Version-deletion outcome. Best-effort auxiliary sweeps are excluded.

## Properties

### bytesRemoved

```ts
bytesRemoved: null | number;
```

***

### failedDeletes

```ts
failedDeletes: null | number;
```

Failed object deletions, if measured. Successful removals are retained.

***

### formatVersion

```ts
formatVersion: number;
```

***

### minAgeSeconds

```ts
minAgeSeconds: number;
```

***

### oldVersions

```ts
oldVersions: null | number;
```

***

### statisticsComplete

```ts
statisticsComplete: boolean;
```

False after uncertain retry/recovery; both counts are then null.
