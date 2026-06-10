[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / IndexConfig

# Interface: IndexConfig

A description of an index currently configured on a column

## Properties

### columns

```ts
columns: string[];
```

The columns in the index

Currently this is always an array of size 1. In the future there may
be more columns to represent composite indices.

***

### createdAt?

```ts
optional createdAt: number;
```

When the index was created, as milliseconds since Unix epoch.

`undefined` for remote tables or indices created before timestamps were tracked.

***

### indexDetails?

```ts
optional indexDetails: string;
```

Index-type-specific details, serialized as JSON.

`undefined` for remote tables or when details are unavailable.

***

### indexType

```ts
indexType: string;
```

The type of the index

***

### indexUuid?

```ts
optional indexUuid: string;
```

The UUID of the first segment of the index.

`undefined` for remote tables, which do not yet surface this.

***

### indexVersion?

```ts
optional indexVersion: number;
```

The on-disk index format version.

`undefined` for remote tables.

***

### name

```ts
name: string;
```

The name of the index

***

### numIndexedRows?

```ts
optional numIndexedRows: number;
```

The number of rows indexed, across all segments.

`undefined` for remote tables.

***

### numSegments?

```ts
optional numSegments: number;
```

The number of segments that make up the index.

`undefined` for remote tables.

***

### numUnindexedRows?

```ts
optional numUnindexedRows: number;
```

The number of rows not yet covered by this index.

`undefined` for remote tables.

***

### sizeBytes?

```ts
optional sizeBytes: number;
```

The total size in bytes of all index files across all segments.

`undefined` for remote tables or indices without size tracking.

***

### typeUrl?

```ts
optional typeUrl: string;
```

The protobuf type URL, a precise type identifier for the index.

`undefined` for remote tables.
