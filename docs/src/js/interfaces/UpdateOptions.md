[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / UpdateOptions

# Interface: UpdateOptions

## Table of contents

### Properties

- [where](UpdateOptions.md#where)

## Properties

### where

• **where**: `string`

A filter that limits the scope of the update.

This should be an SQL filter expression.

Only rows that satisfy the expression will be updated.

For example, this could be 'my_col == 0' to replace all instances
of 0 in a column with some other default value.

#### Defined in

[table.ts:50](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L50)
