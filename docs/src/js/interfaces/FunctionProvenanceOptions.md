[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionProvenanceOptions

# Interface: FunctionProvenanceOptions

Snapshot and record limit for a producing-version listing.

## Properties

### limit?

```ts
optional limit: number;
```

Maximum records to return (server default 10000, cap 100000).

***

### version?

```ts
optional version: number | bigint;
```

An exact table version. Defaults to the table handle's read version.
