[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ColumnAlteration

# Interface: ColumnAlteration

A definition of a column alteration. The alteration changes the column at
`path` to have the new name `name`, to be nullable if `nullable` is true,
and to have the data type `data_type`. At least one of `rename` or `nullable`
must be provided.

## Properties

### nullable?

> `optional` **nullable**: `boolean`

Set the new nullability. Note that a nullable column cannot be made non-nullable.

***

### path

> **path**: `string`

The path to the column to alter. This is a dot-separated path to the column.
If it is a top-level column then it is just the name of the column. If it is
a nested column then it is the path to the column, e.g. "a.b.c" for a column
`c` nested inside a column `b` nested inside a column `a`.

***

### rename?

> `optional` **rename**: `string`

The new name of the column. If not provided then the name will not be changed.
This must be distinct from the names of all other columns in the table.
