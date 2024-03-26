# Rust-backed Client Migration Guide

In an effort to ensure all clients have the same set of capabilities we have begun migrating the
python and node clients onto a common Rust base library. In python, this new client is part of
the same lancedb package, exposed as an asynchronous client. Once the asynchronous client has
reached full functionality we will begin migrating the synchronous library to be a thin wrapper
around the asynchronous client.

This guide describes the differences between the two APIs and will hopefully assist users
that would like to migrate to the new API.

## Closeable Connections

The Connection now has a `close` method. You can call this when
you are done with the connection to eagerly free resources. Currently
this is limited to freeing/closing the HTTP connection for remote
connections. In the future we may add caching or other resources to
native connections so this is probably a good practice even if you
aren't using remote connections.

In addition, the connection can be used as a context manager which may
be a more convenient way to ensure the connection is closed.

```python
import lancedb

async def my_async_fn():
    with await lancedb.connect_async("my_uri") as db:
        print(await db.table_names())
```

It is not mandatory to call the `close` method. If you do not call it
then the connection will be closed when the object is garbage collected.

## Closeable Table

The Table now also has a `close` method, similar to the connection. This
can be used to eagerly free the cache used by a Table object. Similar to
the connection, it can be used as a context manager and it is not mandatory
to call the `close` method.

### Changes to Table APIs

- Previously `Table.schema` was a property. Now it is an async method.
- The method `Table.__len__` was removed and `len(table)` will no longer
  work. Use `Table.count_rows` instead.

### Creating Indices

The `Table.create_index` method is now used for creating both vector indices
and scalar indices. It currently requires a column name to be specified (the
column to index). Vector index defaults are now smarter and scale better with
the size of the data.

To specify index configuration details you will need to specify which kind of
index you are using.

### Querying

The `Table.search` method has been renamed to `AsyncTable.vector_search` for
clarity.

## Features not yet supported

The following features are not yet supported by the asynchronous API. However,
we plan to support them soon.

- You cannot specify an embedding function when creating or opening a table.
  You must calculate embeddings yourself if using the asynchronous API
- The merge insert operation is not supported in the asynchronous API
- Cleanup / compact / optimize indices are not supported in the asynchronous API
- add / alter columns is not supported in the asynchronous API
- The asynchronous API does not yet support any full text search or reranking
  search
- Remote connections to LanceDb Cloud are not yet supported.
- The method Table.head is not yet supported.
