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

The total size, in bytes, of the table's data files

Read from the manifest, so this excludes index files, deletion files,
overlay files, and manifests.
