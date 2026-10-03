[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ListTablesOptions

# Interface: ListTablesOptions

## Properties

### limit?

```ts
optional limit: number;
```

An upper bound on how many tables to return.

A page may hold fewer than this and still not be the last one, so keep
going while the response carries a page token rather than while pages are
full.

***

### pageToken?

```ts
optional pageToken: string;
```

Token from a previous response, to resume listing where it left off.

The token is opaque: it carries whatever the database needs to resume, and
callers should not construct or interpret one.
