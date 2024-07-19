[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableNamesOptions

# Interface: TableNamesOptions

## Properties

### limit?

> `optional` **limit**: `number`

An optional limit to the number of results to return.

***

### startAfter?

> `optional` **startAfter**: `string`

If present, only return names that come lexicographically after the
supplied value.

This can be combined with limit to implement pagination by setting this to
the last table name from the previous page.
