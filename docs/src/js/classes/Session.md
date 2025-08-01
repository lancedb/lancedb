[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Session

# Class: Session

A session for managing caches and object stores across LanceDB operations.

Sessions allow you to configure cache sizes for index and metadata caches,
which can significantly impact memory use and performance. They can
also be re-used across multiple connections to share the same cache state.

## Constructors

### new Session()

```ts
new Session(indexCacheSizeBytes?, metadataCacheSizeBytes?): Session
```

Create a new session with custom cache sizes.

# Parameters

- `index_cache_size_bytes`: The size of the index cache in bytes.
  Index data is stored in memory in this cache to speed up queries.
  Defaults to 6GB if not specified.
- `metadata_cache_size_bytes`: The size of the metadata cache in bytes.
  The metadata cache stores file metadata and schema information in memory.
  This cache improves scan and write performance.
  Defaults to 1GB if not specified.

#### Parameters

* **indexCacheSizeBytes?**: `null` \| `bigint`

* **metadataCacheSizeBytes?**: `null` \| `bigint`

#### Returns

[`Session`](Session.md)

## Methods

### approxNumItems()

```ts
approxNumItems(): number
```

Get the approximate number of items cached in the session.

#### Returns

`number`

***

### sizeBytes()

```ts
sizeBytes(): bigint
```

Get the current size of the session caches in bytes.

#### Returns

`bigint`

***

### default()

```ts
static default(): Session
```

Create a session with default cache sizes.

This is equivalent to creating a session with 6GB index cache
and 1GB metadata cache.

#### Returns

[`Session`](Session.md)
