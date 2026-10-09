[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableNamesOptions

# Interface: ~~TableNamesOptions~~

## Deprecated

Use [ListTablesOptions](ListTablesOptions.md) with [Connection.listTables](../classes/Connection.md#listtables)
instead.

## Properties

### ~~limit?~~

```ts
optional limit: number;
```

The maximum number of names to return. Omitted returns all names; zero returns none.

***

### ~~startAfter?~~

```ts
optional startAfter: string;
```

If present, only return names that come lexicographically after the
supplied value.

This can be combined with limit to implement pagination by setting this to
the last table name from the previous page.
