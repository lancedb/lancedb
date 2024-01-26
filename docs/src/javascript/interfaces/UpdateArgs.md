[vectordb](../README.md) / [Exports](../modules.md) / UpdateArgs

# Interface: UpdateArgs

## Table of contents

### Properties

- [values](UpdateArgs.md#values)
- [where](UpdateArgs.md#where)

## Properties

### values

• **values**: `Record`\<`string`, `Literal`\>

A key-value map of updates. The keys are the column names, and the values are the
new values to set

#### Defined in

[index.ts:461](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L461)

___

### where

• `Optional` **where**: `string`

A filter in the same format used by a sql WHERE clause. The filter may be empty,
in which case all rows will be updated.

#### Defined in

[index.ts:455](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L455)
