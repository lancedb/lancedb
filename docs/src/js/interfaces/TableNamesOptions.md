[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / TableNamesOptions

# Interface: TableNamesOptions

## Table of contents

### Properties

- [limit](TableNamesOptions.md#limit)
- [startAfter](TableNamesOptions.md#startafter)

## Properties

### limit

• `Optional` **limit**: `number`

An optional limit to the number of results to return.

#### Defined in

[connection.ts:48](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L48)

___

### startAfter

• `Optional` **startAfter**: `string`

If present, only return names that come lexicographically after the
supplied value.

This can be combined with limit to implement pagination by setting this to
the last table name from the previous page.

#### Defined in

[connection.ts:46](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/connection.ts#L46)
