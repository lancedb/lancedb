[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MaterializedViewDefinition

# Interface: MaterializedViewDefinition

The query that defines a materialized view.

## Properties

### filter?

```ts
optional filter: string;
```

SQL predicate selecting the source rows the view holds.

***

### inputs

```ts
inputs: string[];
```

Source columns the projections and filter read.

***

### limit?

```ts
optional limit: number;
```

Cap on the number of rows the view holds.

***

### projections

```ts
projections: [string, string][];
```

`[output column, SQL expression]` pairs, in view schema order.

***

### sourceTable

```ts
sourceTable: string;
```

Name of the source table, in the same database as the view.
