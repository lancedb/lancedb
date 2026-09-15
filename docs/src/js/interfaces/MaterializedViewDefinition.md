[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MaterializedViewDefinition

# Interface: MaterializedViewDefinition

The query that defines a materialized view, as stored:
`SELECT columns FROM [ns.]table [, function(args) AS alias | , UNNEST(column) AS alias]
[WHERE predicate] [LIMIT n]`. A Function in `FROM` position yields one row
per element it returns.

## Properties

### query

```ts
query: string;
```

The defining query, in the canonical spelling the server stores.
