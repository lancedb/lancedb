[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / IndexOptions

# Interface: IndexOptions

## Properties

### config?

```ts
optional config: Index;
```

Advanced index configuration

This option allows you to specify a specific index to create and also
allows you to pass in configuration for training the index.

See the static methods on Index for details on the various index types.

If this is not supplied then column data type(s) and column statistics
will be used to determine the most useful kind of index to create.

***

### name?

```ts
optional name: string;
```

Optional custom name for the index.

Custom names must contain 1–128 ASCII letters, digits, dashes, underscores,
or periods, excluding `.` and `..`.

If not provided, a default name will be generated based on the column name.
Existing and generated names remain addressable through index operations,
except that remote tables reject empty names, `.` and `..` to prevent URL
path normalization. Existing `.` or `..` indices on managed tables require
server-side cleanup, or a native connection with direct dataset access.

***

### replace?

```ts
optional replace: boolean;
```

Whether to replace the existing index

If this is false, and another index already exists on the same columns
and the same name, then an error will be returned.  This is true even if
that index is out of date.

The default is true

***

### train?

```ts
optional train: boolean;
```

Whether to train the index with existing data.

If true (default), the index will be trained with existing data in the table.
If false, the index will be created empty and populated as new data is added.

Note: This option is only supported for scalar indices. Vector indices always train.

***

### waitTimeoutSeconds?

```ts
optional waitTimeoutSeconds: number;
```

Timeout in seconds to wait for index creation to complete.

Only applies to LanceDB Enterprise tables, which build the index in the
background.
If not specified, `createIndex` returns once the build is scheduled.
LanceDB Embedded tables always build the index before `createIndex` returns.
