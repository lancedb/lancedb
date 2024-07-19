[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / UpdateOptions

# Interface: UpdateOptions

## Properties

### where

> **where**: `string`

A filter that limits the scope of the update.

This should be an SQL filter expression.

Only rows that satisfy the expression will be updated.

For example, this could be 'my_col == 0' to replace all instances
of 0 in a column with some other default value.
