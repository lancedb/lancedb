[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LsmWriteSpec

# Interface: LsmWriteSpec

Specification selecting Lance's MemWAL LSM-style write path for
`mergeInsert`.

`specType` is either `"bucket"` or `"unsharded"`. For `"bucket"`,
`column` and `numBuckets` are required.

## Properties

### column?

```ts
optional column: string;
```

Bucket variant: the unenforced primary key column to hash-bucket.

***

### maintainedIndexes?

```ts
optional maintainedIndexes: string[];
```

Names of indexes the MemWAL should keep up to date during writes.

***

### numBuckets?

```ts
optional numBuckets: number;
```

Bucket variant: the number of buckets, in `[1, 1024]`.

***

### specType

```ts
specType: "bucket" | "unsharded";
```

One of `"bucket"` or `"unsharded"`.
