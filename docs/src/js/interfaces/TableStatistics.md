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
("meta", "meta.geo", "meta.geo.lat"). A struct's subfields get their own
entries *in addition to* the struct's, so sum only the top-level entries
to reach `totalBytes` — summing every entry double-counts. Undefined
when the backend provides no per-column breakdown (e.g. older remote
servers).

For the full contract — path quoting, list elements, and which bytes are
excluded — see `TableStatistics::column_bytes` at
<https://docs.rs/lancedb/latest/lancedb/table/struct.TableStatistics.html>.

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
