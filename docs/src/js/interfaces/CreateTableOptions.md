[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / CreateTableOptions

# Interface: CreateTableOptions

## Table of contents

### Properties

- [existOk](CreateTableOptions.md#existok)
- [mode](CreateTableOptions.md#mode)

## Properties

### existOk

• **existOk**: `boolean`

If this is true and the table already exists and the mode is "create"
then no error will be raised.

#### Defined in

[connection.ts:35](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L35)

___

### mode

• **mode**: ``"overwrite"`` \| ``"create"``

The mode to use when creating the table.

If this is set to "create" and the table already exists then either
an error will be thrown or, if existOk is true, then nothing will
happen.  Any provided data will be ignored.

If this is set to "overwrite" then any existing table will be replaced.

#### Defined in

[connection.ts:30](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L30)
