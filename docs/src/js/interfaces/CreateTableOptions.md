[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CreateTableOptions

# Interface: CreateTableOptions

## Properties

### ~~dataStorageVersion?~~

```ts
optional dataStorageVersion: string;
```

The version of the data storage format to use.

The default is `stable`.
Set to "legacy" to use the old format.

#### Deprecated

Pass `new_table_data_storage_version` to storageOptions instead.

***

### embeddingFunction?

```ts
optional embeddingFunction: EmbeddingFunctionConfig;
```

***

### ~~enableV2ManifestPaths?~~

```ts
optional enableV2ManifestPaths: boolean;
```

Use the new V2 manifest paths. These paths provide more efficient
opening of datasets with many versions on object stores.  WARNING:
turning this on will make the dataset unreadable for older versions
of LanceDB (prior to 0.10.0). To migrate an existing dataset, instead
use the LocalTable#migrateManifestPathsV2 method.

#### Deprecated

Pass `new_table_enable_v2_manifest_paths` to storageOptions instead.

***

### existOk

```ts
existOk: boolean;
```

If this is true and the table already exists and the mode is "create"
then no error will be raised.

***

### mode

```ts
mode: "overwrite" | "create";
```

The mode to use when creating the table.

If this is set to "create" and the table already exists then either
an error will be thrown or, if existOk is true, then nothing will
happen.  Any provided data will be ignored.

If this is set to "overwrite" then any existing table will be replaced.

***

### schema?

```ts
optional schema: SchemaLike;
```

***

### storageOptions?

```ts
optional storageOptions: Record<string, string>;
```

Configuration for object storage.

Options already set on the connection will be inherited by the table,
but can be overridden here.

The available options are described at https://lancedb.github.io/lancedb/guides/storage/
