[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / AddDataOptions

# Interface: AddDataOptions

Options for adding data to a table.

## Table of contents

### Properties

- [mode](AddDataOptions.md#mode)

## Properties

### mode

• **mode**: ``"append"`` \| ``"overwrite"``

If "append" (the default) then the new data will be added to the table

If "overwrite" then the new data will replace the existing data in the table.

#### Defined in

[table.ts:36](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/table.ts#L36)
