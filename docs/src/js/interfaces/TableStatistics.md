[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableStatistics

# Interface: TableStatistics

## Properties

### columnBytes?

```ts
optional columnBytes: Record<string, number>;
```

The compressed on-disk bytes of each column, keyed by dotted field path
("meta", "meta.geo", "meta.geo.lat"). Every nesting level gets an entry
covering the field's own bytes plus its whole subtree, so a struct
reports its total while its children report the breakdown (the
top-level entries alone sum to `totalBytes`). Counts data-file bytes
only: blob sidecar payloads and index files are not included, and blob
columns therefore report just their descriptor bytes. Undefined when
the backend provides no per-column breakdown (e.g. older remote
servers).

***

### fragmentStats

```ts
fragmentStats: FragmentStatistics;
```

Statistics on table fragments

***

### numIndices

```ts
numIndices: number;
```

The number of indices in the table

***

### numRows

```ts
numRows: number;
```

The number of rows in the table

***

### totalBytes

```ts
totalBytes: number;
```

The total number of bytes in the table
