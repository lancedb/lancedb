[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / LsmWriteSpec

# Interface: LsmWriteSpec

Specification selecting Lance's MemWAL LSM-style write path for
`mergeInsert`.

`specType` is `"bucket"`, `"identity"`, or `"unsharded"`. For `"bucket"`,
`column` and `numBuckets` are required; for `"identity"`, `column` is
required.

## Properties

### column?

```ts
optional column: string;
```

Bucket and identity variants: the sharding column.

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
specType: "bucket" | "identity" | "unsharded";
```

One of `"bucket"`, `"identity"`, or `"unsharded"`.

***

### writerConfigDefaults?

```ts
optional writerConfigDefaults: Record<string, string>;
```

Default `ShardWriter` configuration recorded in the MemWAL index.
