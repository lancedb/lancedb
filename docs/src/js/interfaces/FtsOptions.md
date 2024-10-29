[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FtsOptions

# Interface: FtsOptions

Options to create an `FTS` index

## Properties

### withPosition?

> `optional` **withPosition**: `boolean`

Whether to store the positions of the term in the document.

If this is true then the index will store the positions of the term in the document.
This allows phrase queries to be run. But it also increases the size of the index,
and the time to build the index.

The default value is true.

***
