[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ColumnAlteration

# Interface: ColumnAlteration

A definition of a column alteration. The alteration changes the column at
`path` to have the new name `name`, to be nullable if `nullable` is true,
and to have the data type `data_type`. At least one of `rename` or `nullable`
must be provided.

## Properties

### dataType?

```ts
optional dataType: string;
```

A new data type for the column. If not provided then the data type will not be changed.
Changing data types is limited to casting to the same general type. For example, these
changes are valid:
* `int32` -> `int64` (integers)
* `double` -> `float` (floats)
* `string` -> `large_string` (strings)
But these changes are not:
* `int32` -> `double` (mix integers and floats)
* `string` -> `int32` (mix strings and integers)

***

### nullable?

```ts
optional nullable: boolean;
```

Set the new nullability. Note that a nullable column cannot be made non-nullable.

***

### path

```ts
path: string;
```

The path to the column to alter. This is a dot-separated path to the column.
If it is a top-level column then it is just the name of the column. If it is
a nested column then it is the path to the column, e.g. "a.b.c" for a column
`c` nested inside a column `b` nested inside a column `a`.

***

### rename?

```ts
optional rename: string;
```

The new name of the column. If not provided then the name will not be changed.
This must be distinct from the names of all other columns in the table.
