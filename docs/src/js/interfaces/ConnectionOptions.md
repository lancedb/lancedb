[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ConnectionOptions

# Interface: ConnectionOptions

## Properties

### readConsistencyInterval?

> `optional` **readConsistencyInterval**: `number`

(For LanceDB OSS only): The interval, in seconds, at which to check for
updates to the table from other processes. If None, then consistency is not
checked. For performance reasons, this is the default. For strong
consistency, set this to zero seconds. Then every read will check for
updates from other processes. As a compromise, you can set this to a
non-zero value for eventual consistency. If more than that interval
has passed since the last check, then the table will be checked for updates.
Note: this consistency only applies to read operations. Write operations are
always consistent.

***

### storageOptions?

> `optional` **storageOptions**: `Record`&lt;`string`, `string`&gt;

(For LanceDB OSS only): configuration for object storage.

The available options are described at https://lancedb.github.io/lancedb/guides/storage/
