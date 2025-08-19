[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TableMetadata

# Class: TableMetadata

Manager for table metadata operations.

Table metadata allows storing arbitrary key-value information about
the table such as tags, descriptions, or configuration settings.

## Methods

### deleteKeys()

```ts
deleteKeys(keys): Promise<void>
```

Delete specific keys from table metadata.

#### Parameters

* **keys**: `string`[]
    List of metadata keys to delete

#### Returns

`Promise`&lt;`void`&gt;

***

### get()

```ts
get(): Promise<Record<string, string>>
```

Retrieve all table metadata as key-value pairs.

#### Returns

`Promise`&lt;`Record`&lt;`string`, `string`&gt;&gt;

Dictionary containing all table metadata

***

### insert()

```ts
insert(metadata): Promise<void>
```

Insert or update table metadata.

#### Parameters

* **metadata**: `Record`&lt;`string`, `string`&gt;
    Dictionary of key-value pairs to insert or update

#### Returns

`Promise`&lt;`void`&gt;
