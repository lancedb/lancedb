[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableStatistics

# Interface: TableStatistics

## Properties

### fragmentStats

```ts
fragmentStats: FragmentStatistics;
```

Statistics on table fragments

***

### numDeletedRows?

```ts
optional numDeletedRows: number;
```

The number of rows marked as deleted across all fragments of the table

These rows are not included in `numRows`, but still occupy space on disk
until the table is compacted, so a large value here indicates that the
table should be optimized. Fragments in which every row was deleted are
dropped outright, so their rows are not counted here.

Absent (`undefined`) when the backend does not report deletion counts.

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

The total size, in bytes, of the table's data files, index files, and
overlay files

Read from the manifest, so this excludes deletion files and manifests.
