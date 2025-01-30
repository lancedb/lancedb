The merge insert command is a flexible API that can be used to perform:

1. Upsert
2. Insert-if-not-exists
3. Replace range

It works by joining the input data with the target table on a key you provide.
Often this key is a unique row id key. You can then specify what to do when
there is a match and when there is not a match. For example, for upsert you want
to update if the row has a match and insert if the row doesn't have a match.
Whereas for insert-if-not-exists you only want to insert if the row doesn't have
a match.

You can also read more in the API reference:

* Python
    * Sync: [lancedb.table.Table.merge_insert][]
    * Async: [lancedb.table.AsyncTable.merge_insert][]
* Typescript: [lancedb.Table.mergeInsert](../../js/classes/Table.md/#mergeinsert)

!!! tip "Use scalar indices to speed up merge insert"

    The merge insert command needs to perform a join between the input data and the
    target table on the `on` key you provide. This requires scanning that entire
    column, which can be expensive for large tables. To speed up this operation,
    you can create a scalar index on the `on` column, which will allow LanceDB to
    find matches without having to scan the whole tables.

    Read more about scalar indices in [Building a Scalar Index](../scalar_index.md)
    guide.

<!--
Things to note:
* Generally use some unique key. Need to manage your own, LanceDB doens't have
    a built-in primary key. Having duplicates on both sides of join can be bad.
* Add a scalar B-Tree or Bitmap index on the unique key to speed up the upsert.
* You can provide subsets of columns or all columns in the upsert.
-->
## Upsert

Upsert updates rows if they exist and inserts them if they don't. To do this
with merge insert, enable both `when_matched_update_all()` and
`when_not_matched_insert_all()`.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:upsert_basic"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:upsert_basic_async"
        ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```typescript
        --8<-- "nodejs/examples/merge_insert.test.ts:upsert_basic"
        ```

!!! note "Providing subsets of columns"

    If a column is nullable, it can be omitted from input data and it will be
    considered `null`. Columns can also be provided in any order.

## Insert-if-not-exists

To avoid inserting duplicate rows, you can use the insert-if-not-exists command.
This will only insert rows that do not have a match in the target table. To do
this with merge insert, enable just `when_not_matched_insert_all()`.


=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:insert_if_not_exists"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:insert_if_not_exists_async"
        ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```typescript
        --8<-- "nodejs/examples/merge_insert.test.ts:insert_if_not_exists"
        ```


## Replace range

You can also replace a range of rows in the target table with the input data.
For example, if you have a table of document chunks, where each chunk has
both a `doc_id` and a `chunk_id`, you can replace all chunks for a given
`doc_id` with updated chunks. This can be tricky otherwise because if you
try to use upsert when the new data has fewer chunks you will end up with
extra chunks. To avoid this, add another clause to delete any chunks for
the document that are not in the new data, with
`when_not_matched_by_source_delete`.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:replace_range"
        ```

    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_merge_insert.py:replace_range_async"
        ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```typescript
        --8<-- "nodejs/examples/merge_insert.test.ts:replace_range"
        ```
