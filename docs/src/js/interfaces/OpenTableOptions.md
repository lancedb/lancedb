[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OpenTableOptions

# Interface: OpenTableOptions

## Properties

### ~~indexCacheSize?~~

```ts
optional indexCacheSize: number;
```

Set the size of the index cache, specified as a number of entries

#### Deprecated

Use session-level cache configuration instead.
Create a Session with custom cache sizes and pass it to the connect() function.

The exact meaning of an "entry" will depend on the type of index:
- IVF: there is one entry for each IVF partition
- BTREE: there is one entry for the entire index

This cache applies to the entire opened table, across all indices.
Setting this value higher will increase performance on larger datasets
at the expense of more RAM

***

### ref?

```ts
optional ref: OpenTableRefOptions;
```

Optional reference selector used to choose the initial table timeline or
snapshot when opening a table.

Branches are selected here. After the table is opened, [Table.checkout](../classes/Table.md#checkout)
only supports version numbers and tags on the current branch timeline.

***

### storageOptions?

```ts
optional storageOptions: Record<string, string>;
```

Configuration for object storage.

Options already set on the connection will be inherited by the table,
but can be overridden here.

The available options are described at https://lancedb.com/docs/storage/
