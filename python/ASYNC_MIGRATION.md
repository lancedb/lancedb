# Migration from Sync to Async API

A new asynchronous API has been added to LanceDb.  This API is built
on top of the rust lancedb crate (instead of being built on top of
pylance).  This will help keep the various language bindings in sync.
There are some slight changes between the synchronous and the asynchronous
APIs.  This document will help you migrate.  These changes relate mostly
to the Connection and Table classes.

## Almost all functions are async

The most important change is that almost all functions are now async.
This means the functions now return `asyncio` coroutines.  You will
need to use `await` to call these functions.

## Connection

* The connection now has a `close` method.  You can call this when
  you are done with the connection to eagerly free resources.  Currently
  this is limited to freeing/closing the HTTP connection for remote
  connections.  In the future we may add caching or other resources to
  native connections so this is probably a good practice even if you aren't using remote connections.

  In addition, the connection can be used as a context manager which may
  be a more convenient way to ensure the connection is closed.

  It is not mandatory to call the `close` method.  If you don't call it
  the connection will be closed when the object is garbage collected.

## Table

* The table now has a `close` method, similar to the connection.  This
  can be used to eagerly free the cache used by a Table object.  Similar
  to the connection, it can be used as a context manager and it is not
  mandatory to call the `close` method.
* Previously `Table.schema` was a property.  Now it is an async method.
* The method `Table.__len__` was removed and `len(table)` will no longer
  work.  Use `Table.count_rows` instead.
