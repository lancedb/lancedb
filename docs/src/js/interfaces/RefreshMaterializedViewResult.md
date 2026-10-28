[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / RefreshMaterializedViewResult

# Interface: RefreshMaterializedViewResult

## Properties

### mode?

```ts
optional mode: string;
```

How the view was brought up to date: "rebuild", or absent for LanceDB Distributed SQL refreshes.

***

### rowsWritten?

```ts
optional rowsWritten: number;
```

Rows written by this refresh, or absent for LanceDB Distributed SQL refreshes.

***

### sourceVersion?

```ts
optional sourceVersion: number;
```

Source version refreshed to, or absent for LanceDB Distributed SQL refreshes.

***

### version

```ts
version: number;
```
