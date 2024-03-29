[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / IndexOptions

# Interface: IndexOptions

## Table of contents

### Properties

- [config](IndexOptions.md#config)
- [replace](IndexOptions.md#replace)

## Properties

### config

• `Optional` **config**: [`Index`](../classes/Index.md)

Advanced index configuration

This option allows you to specify a specfic index to create and also
allows you to pass in configuration for training the index.

See the static methods on Index for details on the various index types.

If this is not supplied then column data type(s) and column statistics
will be used to determine the most useful kind of index to create.

#### Defined in

[indices.ts:192](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/indices.ts#L192)

___

### replace

• `Optional` **replace**: `boolean`

Whether to replace the existing index

If this is false, and another index already exists on the same columns
and the same name, then an error will be returned.  This is true even if
that index is out of date.

The default is true

#### Defined in

[indices.ts:202](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/indices.ts#L202)
