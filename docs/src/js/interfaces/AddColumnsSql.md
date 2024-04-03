[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / AddColumnsSql

# Interface: AddColumnsSql

A definition of a new column to add to a table.

## Table of contents

### Properties

- [name](AddColumnsSql.md#name)
- [valueSql](AddColumnsSql.md#valuesql)

## Properties

### name

• **name**: `string`

The name of the new column.

#### Defined in

native.d.ts:43

___

### valueSql

• **valueSql**: `string`

The values to populate the new column with, as a SQL expression.
The expression can reference other columns in the table.

#### Defined in

native.d.ts:48
