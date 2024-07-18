[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / CreateTableOptions

# Interface: CreateTableOptions

## Table of contents

### Properties

- [embeddingFunction](CreateTableOptions.md#embeddingfunction)
- [existOk](CreateTableOptions.md#existok)
- [mode](CreateTableOptions.md#mode)
- [schema](CreateTableOptions.md#schema)
- [storageOptions](CreateTableOptions.md#storageoptions)
- [useLegacyFormat](CreateTableOptions.md#uselegacyformat)

## Properties

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunctionConfig`](embedding.EmbeddingFunctionConfig.md)

#### Defined in

[connection.ts:54](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L54)

___

### existOk

• **existOk**: `boolean`

If this is true and the table already exists and the mode is "create"
then no error will be raised.

#### Defined in

[connection.ts:36](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L36)

___

### mode

• **mode**: ``"overwrite"`` \| ``"create"``

The mode to use when creating the table.

If this is set to "create" and the table already exists then either
an error will be thrown or, if existOk is true, then nothing will
happen.  Any provided data will be ignored.

If this is set to "overwrite" then any existing table will be replaced.

#### Defined in

[connection.ts:31](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L31)

___

### schema

• `Optional` **schema**: `SchemaLike`

#### Defined in

[connection.ts:53](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L53)

___

### storageOptions

• `Optional` **storageOptions**: `Record`\<`string`, `string`\>

Configuration for object storage.

Options already set on the connection will be inherited by the table,
but can be overridden here.

The available options are described at https://lancedb.github.io/lancedb/guides/storage/

#### Defined in

[connection.ts:46](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L46)

___

### useLegacyFormat

• `Optional` **useLegacyFormat**: `boolean`

If true then data files will be written with the legacy format

The default is true while the new format is in beta

#### Defined in

[connection.ts:52](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/connection.ts#L52)
