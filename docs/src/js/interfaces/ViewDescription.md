[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ViewDescription

# Interface: ViewDescription

What a database records about one view.

A view holds no rows: it stores the statement that defines it and the
schema that statement resolved to, so a reader sees its sources as they
are at read time.

## Properties

### defaultDatabase

```ts
defaultDatabase: string;
```

The database that unqualified table names in [query](ViewDescription.md#query) resolve against.

***

### name

```ts
name: string;
```

The view's name within its namespace.

***

### namespacePath

```ts
namespacePath: string[];
```

The namespace holding the view; empty is the root namespace.

***

### query

```ts
query: string;
```

The defining query, as the database stores it.

***

### schema

```ts
schema: Schema<any>;
```

The schema the defining query resolved to when the view was created.
