[vectordb](../README.md) / [Exports](../modules.md) / UpdateSqlArgs

# Interface: UpdateSqlArgs

## Table of contents

### Properties

- [valuesSql](UpdateSqlArgs.md#valuessql)
- [where](UpdateSqlArgs.md#where)

## Properties

### valuesSql

• **valuesSql**: `Record`\<`string`, `string`\>

A key-value map of updates. The keys are the column names, and the values are the
new values to set as SQL expressions.

#### Defined in

[index.ts:334](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L334)

___

### where

• `Optional` **where**: `string`

A filter in the same format used by a sql WHERE clause. The filter may be empty,
in which case all rows will be updated.

#### Defined in

[index.ts:328](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L328)
