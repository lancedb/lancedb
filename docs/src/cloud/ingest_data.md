## Insert data
The LanceDB Cloud SDK for data ingestion remains consistent with our open-source version, 
ensuring a seamless transition for existing OSS users. 
!!! note "unsupported parameters in create_table"

    The following two parameters: `mode="overwrite"` and `exist_ok`, are expected to be added by Nov, 2024.
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:import-ingest-data"
    
    --8<-- "python/python/tests/docs/test_cloud.py:ingest_data"
    ```
=== "Typescript"

    ```typescript 
    --8<-- "nodejs/examples/cloud.test.ts:imports"
    --8<-- "nodejs/examples/cloud.test.ts:ingest_data"
    ```

## Insert large datasets
It is recommended to use itertators to add large datasets in batches when creating 
your table in one go. Data will be automatically compacted for the best query performance.
!!! info "batch size"

    The batch size .
=== "Python"

    ```python
    --8<-- "python/python/tests/docs/test_cloud.py:ingest_data_in_batch"
    ```