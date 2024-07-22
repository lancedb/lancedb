[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CreateTableOptions

# Interface: CreateTableOptions

## Properties

### embeddingFunction?

> `optional` **embeddingFunction**: [`EmbeddingFunctionConfig`](../namespaces/embedding/interfaces/EmbeddingFunctionConfig.md)

***

### existOk

> **existOk**: `boolean`

If this is true and the table already exists and the mode is "create"
then no error will be raised.

***

### mode

> **mode**: `"overwrite"` \| `"create"`

The mode to use when creating the table.

If this is set to "create" and the table already exists then either
an error will be thrown or, if existOk is true, then nothing will
happen.  Any provided data will be ignored.

If this is set to "overwrite" then any existing table will be replaced.

***

### schema?

> `optional` **schema**: `SchemaLike`

***

### storageOptions?

> `optional` **storageOptions**: `Record`&lt;`string`, `string`&gt;

Configuration for object storage.

Options already set on the connection will be inherited by the table,
but can be overridden here.

The available options are described at https://lancedb.github.io/lancedb/guides/storage/

***

### useLegacyFormat?

> `optional` **useLegacyFormat**: `boolean`

If true then data files will be written with the legacy format

The default is true while the new format is in beta
