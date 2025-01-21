# Pandas and PyArrow

Because Lance is built on top of [Apache Arrow](https://arrow.apache.org/),
LanceDB is tightly integrated with the Python data ecosystem, including [Pandas](https://pandas.pydata.org/)
and PyArrow. The sequence of steps in a typical workflow is shown below.

## Create dataset

First, we need to connect to a LanceDB database.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-lancedb"
    --8<-- "python/python/tests/docs/test_python.py:connect_to_lancedb"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-lancedb"
    --8<-- "python/python/tests/docs/test_python.py:connect_to_lancedb_async"
    ```

We can load a Pandas `DataFrame` to LanceDB directly.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-pandas"
    --8<-- "python/python/tests/docs/test_python.py:create_table_pandas"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-pandas"
    --8<-- "python/python/tests/docs/test_python.py:create_table_pandas_async"
    ```

Similar to the [`pyarrow.write_dataset()`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html) method, LanceDB's
[`db.create_table()`](python.md/#lancedb.db.DBConnection.create_table) accepts data in a variety of forms.

If you have a dataset that is larger than memory, you can create a table with `Iterator[pyarrow.RecordBatch]` to lazily load the data:

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-iterable"
    --8<-- "python/python/tests/docs/test_python.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_python.py:make_batches"
    --8<-- "python/python/tests/docs/test_python.py:create_table_iterable"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:import-iterable"
    --8<-- "python/python/tests/docs/test_python.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_python.py:make_batches"
    --8<-- "python/python/tests/docs/test_python.py:create_table_iterable_async"
    ```

You will find detailed instructions of creating a LanceDB dataset in
[Getting Started](../basic.md#quick-start) and [API](python.md/#lancedb.db.DBConnection.create_table)
sections.

## Vector search

We can now perform similarity search via the LanceDB Python API.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:vector_search"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:vector_search_async"
    ```

```
    vector     item  price    _distance
0  [5.9, 26.5]  bar   20.0  14257.05957
```

If you have a simple filter, it's faster to provide a `where` clause to LanceDB's `search` method.
For more complex filters or aggregations, you can always resort to using the underlying `DataFrame` methods after performing a search.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:vector_search_with_filter"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_python.py:vector_search_with_filter_async"
    ```
