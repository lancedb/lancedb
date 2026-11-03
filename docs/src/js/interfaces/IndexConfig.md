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
optional createdAt: Date;
```

When the index was created.

`undefined` in LanceDB Enterprise or indices created before timestamps were tracked.

***

### indexDetails?

```ts
optional indexDetails: any;
```

Index-type-specific details parsed as a JavaScript object.

Falls back to a raw string if JSON parsing fails. `undefined` in
LanceDB Enterprise or when details are unavailable.

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

`undefined` in LanceDB Enterprise, which does not yet surface this.

***

### indexVersion?

```ts
optional indexVersion: number;
```

The on-disk index format version.

`undefined` in LanceDB Enterprise.

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

May be `undefined` in LanceDB Enterprise if the server does not report it.

***

### numSegments?

```ts
optional numSegments: number;
```

The number of segments that make up the index.

`undefined` in LanceDB Enterprise.

***

### numUnindexedRows?

```ts
optional numUnindexedRows: number;
```

The number of rows not yet covered by this index.

May be `undefined` in LanceDB Enterprise if the server does not report it.

***

### sizeBytes?

```ts
optional sizeBytes: number;
```

The total size in bytes of all index files across all segments.

`undefined` in LanceDB Enterprise or indices without size tracking.

***

### typeUrl?

```ts
optional typeUrl: string;
```

The protobuf type URL, a precise type identifier for the index.

`undefined` in LanceDB Enterprise.
