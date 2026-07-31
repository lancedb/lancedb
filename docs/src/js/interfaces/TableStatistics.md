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
("meta", "meta.geo", "meta.geo.lat"). A struct's subfields each get
their own entry, and every entry covers the field's own bytes plus its
whole subtree, so a struct reports its total while its children report
the breakdown (the top-level entries alone sum to `totalBytes`). List
elements are not broken out: a list column reports a single total with
its element bytes rolled in. Path segments containing anything other
than letters, digits, or `_` are backtick-quoted, so a subfield named
`a.b` under `meta` is keyed as "meta.`a.b`".

Counts data-file bytes only: blob sidecar payloads and index files are
not included, and blob columns therefore report just their descriptor
bytes. Undefined when the backend provides no per-column breakdown
(e.g. older remote servers).

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
