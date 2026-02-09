[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OptimizeOptions

# Interface: OptimizeOptions

## Properties

### cleanupOlderThan

```ts
cleanupOlderThan: Date;
```

If set then all versions older than the given date
be removed.  The current version will never be removed.
The default is 7 days

#### Example

```ts
// Delete all versions older than 1 day
const olderThan = new Date();
olderThan.setDate(olderThan.getDate() - 1));
tbl.optimize({cleanupOlderThan: olderThan});

// Delete all versions except the current version
tbl.optimize({cleanupOlderThan: new Date()});
```

***

### deleteUnverified

```ts
deleteUnverified: boolean;
```
