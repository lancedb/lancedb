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

No changes yet.

## Table

* Previously `Table.schema` was a property.  Now it is an async method.
* The method `Table.__len__` was removed and `len(table)` will no longer
  work.  Use `Table.count_rows` instead.
