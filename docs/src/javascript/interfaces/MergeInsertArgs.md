[vectordb](../README.md) / [Exports](../modules.md) / MergeInsertArgs

# Interface: MergeInsertArgs

## Table of contents

### Properties

- [whenMatchedUpdateAll](MergeInsertArgs.md#whenmatchedupdateall)
- [whenNotMatchedBySourceDelete](MergeInsertArgs.md#whennotmatchedbysourcedelete)
- [whenNotMatchedInsertAll](MergeInsertArgs.md#whennotmatchedinsertall)

## Properties

### whenMatchedUpdateAll

• `Optional` **whenMatchedUpdateAll**: `string` \| `boolean`

If true then rows that exist in both the source table (new data) and
the target table (old data) will be updated, replacing the old row
with the corresponding matching row.

If there are multiple matches then the behavior is undefined.
Currently this causes multiple copies of the row to be created
but that behavior is subject to change.

Optionally, a filter can be specified.  This should be an SQL
filter where fields with the prefix "target." refer to fields
in the target table (old data) and fields with the prefix
"source." refer to fields in the source table (new data).  For
example, the filter "target.lastUpdated < source.lastUpdated" will
only update matched rows when the incoming `lastUpdated` value is
newer.

Rows that do not match the filter will not be updated.  Rows that
do not match the filter do become "not matched" rows.

#### Defined in

[index.ts:690](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L690)

___

### whenNotMatchedBySourceDelete

• `Optional` **whenNotMatchedBySourceDelete**: `string` \| `boolean`

If true then rows that exist only in the target table (old data)
will be deleted.

If this is a string then it will be treated as an SQL filter and
only rows that both do not match any row in the source table and
match the given filter will be deleted.

This can be used to replace a selection of existing data with
new data.

#### Defined in

[index.ts:707](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L707)

___

### whenNotMatchedInsertAll

• `Optional` **whenNotMatchedInsertAll**: `boolean`

If true then rows that exist only in the source table (new data)
will be inserted into the target table.

#### Defined in

[index.ts:695](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L695)
