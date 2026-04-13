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

Because they may be part of an in-progress transaction, files newer than
7 days old are not deleted by default. If you are sure that there are no
in-progress transactions, then you can set this to true to delete all
files older than `cleanupOlderThan`.

**WARNING**: This should only be set to true if you can guarantee that
no other process is currently working on this dataset. Otherwise the
dataset could be put into a corrupted state.
