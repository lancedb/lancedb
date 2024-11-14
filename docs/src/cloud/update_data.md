LanceDB Cloud efficiently manages updates across many tables. 
Currently, we offer _update_, _merge_insert_, and _delete_.

## update
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:update_data"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    
    --8<-- "nodejs/examples/cloud.test.ts:connect_db_and_open_table"
    --8<-- "nodejs/examples/cloud.test.ts:update_data"
    ```

## merge insert
This merge insert can add rows, update rows, and remove rows all in a single transaction. 
It combines new data from a source table with existing data in a target table by using a join.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:merge_insert"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"

    --8<-- "nodejs/examples/cloud.test.ts:connect_db_and_open_table"
    --8<-- "nodejs/examples/cloud.test.ts:merge_insert"
    ```

## delete
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:delete_data"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"

    --8<-- "nodejs/examples/cloud.test.ts:connect_db_and_open_table"
    --8<-- "nodejs/examples/cloud.test.ts:delete_data"
    ```