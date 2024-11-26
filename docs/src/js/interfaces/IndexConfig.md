[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../README.md) / IndexConfig

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

### indexType

```ts
indexType: string;
```

The type of the index

***

### name

```ts
name: string;
```

The name of the index
