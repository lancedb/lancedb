
<a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/tables_guide.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>

A Table is a collection of Records in a LanceDB Database. Tables in Lance have a schema that defines the columns and their types. These schemas can include nested columns and can evolve over time.

This guide will show how to create tables, insert data into them, and update the data.


## Creating a LanceDB Table

Initialize a LanceDB connection and create a table

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:connect"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:connect_async"
        ```

    LanceDB allows ingesting data from various sources - `dict`, `list[dict]`, `pd.DataFrame`, `pa.Table` or a `Iterator[pa.RecordBatch]`. Let's take a look at some of the these.

=== "Typescript[^1]"

    === "@lancedb/lancedb"

        ```typescript
        import * as lancedb from "@lancedb/lancedb";
        import * as arrow from "apache-arrow";

        const uri = "data/sample-lancedb";
        const db = await lancedb.connect(uri);
        ```

    === "vectordb (deprecated)"

        ```typescript
        const lancedb = require("vectordb");
        const arrow = require("apache-arrow");

        const uri = "data/sample-lancedb";
        const db = await lancedb.connect(uri);
        ```



### From list of tuples or dictionaries

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async"
        ```

    !!! info "Note"
        If the table already exists, LanceDB will raise an error by default.

        `create_table` supports an optional `exist_ok` parameter. When set to True
        and the table exists, then it simply opens the existing table. The data you
        passed in will NOT be appended to the table in that case.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_exist_ok"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_exist_ok"
        ```

    Sometimes you want to make sure that you start fresh. If you want to
    overwrite the table, you can pass in mode="overwrite" to the createTable function.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_overwrite"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_overwrite"
        ```

=== "Typescript[^1]"
    You can create a LanceDB table in JavaScript using an array of records as follows.

    === "@lancedb/lancedb"


        ```ts
        --8<-- "nodejs/examples/basic.test.ts:create_table"
        ```

        This will infer the schema from the provided data. If you want to explicitly provide a schema, you can use `apache-arrow` to declare a schema

        ```ts
        --8<-- "nodejs/examples/basic.test.ts:create_table_with_schema"
        ```

        !!! info "Note"
            `createTable` supports an optional `existsOk` parameter. When set to true
            and the table exists, then it simply opens the existing table. The data you
            passed in will NOT be appended to the table in that case.

        ```ts
        --8<-- "nodejs/examples/basic.test.ts:create_table_exists_ok"
        ```

        Sometimes you want to make sure that you start fresh. If you want to
        overwrite the table, you can pass in mode: "overwrite" to the createTable function.

        ```ts
        --8<-- "nodejs/examples/basic.test.ts:create_table_overwrite"
        ```

    === "vectordb (deprecated)"

        ```ts
        --8<-- "docs/src/basic_legacy.ts:create_table"
        ```

        This will infer the schema from the provided data. If you want to explicitly provide a schema, you can use apache-arrow to declare a schema



        ```ts
        --8<-- "docs/src/basic_legacy.ts:create_table_with_schema"
        ```

        !!! warning
            `existsOk` is not available in `vectordb`



            If the table already exists, vectordb will raise an error by default.
            You can use `writeMode: WriteMode.Overwrite` to overwrite the table.
            But this will delete the existing table and create a new one with the same name.


        Sometimes you want to make sure that you start fresh.

        If you want to overwrite the table, you can pass in `writeMode: lancedb.WriteMode.Overwrite` to the createTable function.

        ```ts
        const table = await con.createTable(tableName, data, {
            writeMode: WriteMode.Overwrite
        })
        ```

### From a Pandas DataFrame


=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pandas"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_from_pandas"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pandas"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_from_pandas"
    ```

!!! info "Note"
    Data is converted to Arrow before being written to disk. For maximum control over how data is saved, either provide the PyArrow schema to convert to or else provide a PyArrow Table directly.

The **`vector`** column needs to be a [Vector](../python/pydantic.md#vector-field) (defined as [pyarrow.FixedSizeList](https://arrow.apache.org/docs/python/generated/pyarrow.list_.html)) type.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_custom_schema"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_custom_schema"
    ```

### From a Polars DataFrame

LanceDB supports [Polars](https://pola.rs/), a modern, fast DataFrame library
written in Rust. Just like in Pandas, the Polars integration is enabled by PyArrow
under the hood. A deeper integration between LanceDB Tables and Polars DataFrames
is on the way.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-polars"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_from_polars"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-polars"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_from_polars"
    ```

### From an Arrow Table
You can also create LanceDB tables directly from Arrow tables.
LanceDB supports float16 data type!

=== "Python"
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-numpy"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_from_arrow_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-polars"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-numpy"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_from_arrow_table"
        ```

=== "Typescript[^1]"

    === "@lancedb/lancedb"

        ```typescript
        --8<-- "nodejs/examples/basic.test.ts:create_f16_table"
        ```

    === "vectordb (deprecated)"

        ```typescript
        --8<-- "docs/src/basic_legacy.ts:create_f16_table"
        ```

### From Pydantic Models

When you create an empty table without data, you must specify the table schema.
LanceDB supports creating tables by specifying a PyArrow schema or a specialized
Pydantic model called `LanceModel`.

For example, the following Content model specifies a table with 5 columns:
`movie_id`, `vector`, `genres`, `title`, and `imdb_id`. When you create a table, you can
pass the class as the value of the `schema` parameter to `create_table`.
The `vector` column is a `Vector` type, which is a specialized Pydantic type that
can be configured with the vector dimensions. It is also important to note that
LanceDB only understands subclasses of `lancedb.pydantic.LanceModel`
(which itself derives from `pydantic.BaseModel`).

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:class-Content"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_from_pydantic"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:class-Content"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_from_pydantic"
    ```

#### Nested schemas

Sometimes your data model may contain nested objects.
For example, you may want to store the document string
and the document source name as a nested Document object:

```python
--8<-- "python/python/tests/docs/test_guide_tables.py:import-pydantic-basemodel"
--8<-- "python/python/tests/docs/test_guide_tables.py:class-Document"
```

This can be used as the type of a LanceDB table column:

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:class-NestedSchema"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_nested_schema"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:class-NestedSchema"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_nested_schema"
    ```
This creates a struct column called "document" that has two subfields
called "content" and "source":

```
In [28]: tbl.schema
Out[28]:
id: string not null
vector: fixed_size_list<item: float>[1536] not null
    child 0, item: float
document: struct<content: string not null, source: string not null> not null
    child 0, content: string not null
    child 1, source: string not null
```

#### Validators

Note that neither Pydantic nor PyArrow automatically validates that input data
is of the correct timezone, but this is easy to add as a custom field validator:

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from lancedb.pydantic import LanceModel
from pydantic import Field, field_validator, ValidationError, ValidationInfo

tzname = "America/New_York"
tz = ZoneInfo(tzname)

class TestModel(LanceModel):
    dt_with_tz: datetime = Field(json_schema_extra={"tz": tzname})

    @field_validator('dt_with_tz')
    @classmethod
    def tz_must_match(cls, dt: datetime) -> datetime:
        assert dt.tzinfo == tz
        return dt

ok = TestModel(dt_with_tz=datetime.now(tz))

try:
    TestModel(dt_with_tz=datetime.now(ZoneInfo("Asia/Shanghai")))
    assert 0 == 1, "this should raise ValidationError"
except ValidationError:
    print("A ValidationError was raised.")
    pass
```

When you run this code it should print "A ValidationError was raised."

#### Pydantic custom types

LanceDB does NOT yet support converting pydantic custom types. If this is something you need,
please file a feature request on the [LanceDB Github repo](https://github.com/lancedb/lancedb/issues/new).

### Using Iterators / Writing Large Datasets

It is recommended to use iterators to add large datasets in batches when creating your table in one go. This does not create multiple versions of your dataset unlike manually adding batches using `table.add()`

LanceDB additionally supports PyArrow's `RecordBatch` Iterators or other generators producing supported data types.

Here's an example using using `RecordBatch` iterator for creating tables.

=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:make_batches"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_from_batch"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
    --8<-- "python/python/tests/docs/test_guide_tables.py:make_batches"
    --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_from_batch"
    ```

You can also use iterators of other types like Pandas DataFrame or Pylists directly in the above example.

## Open existing tables

=== "Python"
    If you forget the name of your table, you can always get a listing of all table names.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:list_tables"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:list_tables_async"
        ```

    Then, you can open any existing tables.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:open_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:open_table_async"
        ```

=== "Typescript[^1]"

    If you forget the name of your table, you can always get a listing of all table names.

    ```typescript
    console.log(await db.tableNames());
    ```

    Then, you can open any existing tables.

    ```typescript
    const tbl = await db.openTable("my_table");
    ```

## Creating empty table
You can create an empty table for scenarios where you want to add data to the table later. An example would be when you want to collect data from a stream/external file and then add it to a table in batches.

=== "Python"


    An empty table can be initialized via a PyArrow schema.
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_empty_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_empty_table_async"
        ```

    Alternatively, you can also use Pydantic to specify the schema for the empty table. Note that we do not
    directly import `pydantic` but instead use `lancedb.pydantic` which is a subclass of `pydantic.BaseModel`
    that has been extended to support LanceDB specific types like `Vector`.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
        --8<-- "python/python/tests/docs/test_guide_tables.py:class-Item"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_empty_table_pydantic"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
        --8<-- "python/python/tests/docs/test_guide_tables.py:class-Item"
        --8<-- "python/python/tests/docs/test_guide_tables.py:create_empty_table_async_pydantic"
        ```

    Once the empty table has been created, you can add data to it via the various methods listed in the [Adding to a table](#adding-to-a-table) section.

=== "Typescript[^1]"

    === "@lancedb/lancedb"

        ```typescript
        --8<-- "nodejs/examples/basic.test.ts:create_empty_table"
        ```

    === "vectordb (deprecated)"

        ```typescript
        --8<-- "docs/src/basic_legacy.ts:create_empty_table"
        ```

## Adding to a table

After a table has been created, you can always add more data to it using the `add` method

=== "Python"
    You can add any of the valid data structures accepted by LanceDB table, i.e, `dict`, `list[dict]`, `pd.DataFrame`, or `Iterator[pa.RecordBatch]`. Below are some examples.

    ### Add a Pandas DataFrame

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_from_pandas"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_async_from_pandas"
        ```

    ### Add a Polars DataFrame

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_from_polars"
        ```
    === "Async API"
    
        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_async_from_polars"
        ```

    ### Add an Iterator

    You can also add a large dataset batch in one go using Iterator of any supported data types.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:make_batches_for_add"
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_from_batch"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:make_batches_for_add"
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_async_from_batch"
        ```

    ### Add a PyArrow table

    If you have data coming in as a PyArrow table, you can add it directly to the LanceDB table.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_from_pyarrow"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_async_from_pyarrow"
        ```

    ### Add a Pydantic Model

    Assuming that a table has been created with the correct schema as shown [above](#creating-empty-table), you can add data items that are valid Pydantic models to the table.

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_from_pydantic"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:add_table_async_from_pydantic"
        ```

    ??? "Ingesting Pydantic models with LanceDB embedding API"
        When using LanceDB's embedding API, you can add Pydantic models directly to the table. LanceDB will automatically convert the `vector` field to a vector before adding it to the table. You need to specify the default value of `vector` field as None to allow LanceDB to automatically vectorize the data.

        === "Sync API"

            ```python
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-embeddings"
            --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_with_embedding"
            ```
        === "Async API"

            ```python
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb-pydantic"
            --8<-- "python/python/tests/docs/test_guide_tables.py:import-embeddings"
            --8<-- "python/python/tests/docs/test_guide_tables.py:create_table_async_with_embedding"
            ```

=== "Typescript[^1]"

    ```javascript
    await tbl.add(
        [
            {vector: [1.3, 1.4], item: "fizz", price: 100.0},
            {vector: [9.5, 56.2], item: "buzz", price: 200.0}
        ]
    )
    ```

## Deleting from a table

Use the `delete()` method on tables to delete rows from a table. To choose which rows to delete, provide a filter that matches on the metadata columns. This can delete any number of rows that match the filter.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_row"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_row_async"
        ```

    ### Deleting row with specific column value

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_specific_row"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_specific_row_async"
        ```
    
    ### Delete from a list of values
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_list_values"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:delete_list_values_async"
        ```

=== "Typescript[^1]"

    ```ts
    await tbl.delete('item = "fizz"')
    ```

    ### Deleting row with specific column value

    ```ts
    const con = await lancedb.connect("./.lancedb")
    const data = [
      {id: 1, vector: [1, 2]},
      {id: 2, vector: [3, 4]},
      {id: 3, vector: [5, 6]},
    ];
    const tbl = await con.createTable("my_table", data)
    await tbl.delete("id = 2")
    await tbl.countRows() // Returns 2
    ```

    ### Delete from a list of values

    ```ts
    const to_remove = [1, 5];
    await tbl.delete(`id IN (${to_remove.join(",")})`)
    await tbl.countRows() // Returns 1
    ```

## Updating a table

This can be used to update zero to all rows depending on how many rows match the where clause. The update queries follow the form of a SQL UPDATE statement. The `where` parameter is a SQL filter that matches on the metadata columns. The `values` or `values_sql` parameters are used to provide the new values for the columns.

| Parameter   | Type | Description |
|---|---|---|
| `where` | `str` | The SQL where clause to use when updating rows. For example, `'x = 2'` or `'x IN (1, 2, 3)'`. The filter must not be empty, or it will error. |
| `values` | `dict` | The values to update. The keys are the column names and the values are the values to set. |
| `values_sql` | `dict` | The values to update. The keys are the column names and the values are the SQL expressions to set. For example, `{'x': 'x + 1'}` will increment the value of the `x` column by 1. |

!!! info "SQL syntax"

    See [SQL filters](../sql.md) for more information on the supported SQL syntax.

!!! warning "Warning"

    Updating nested columns is not yet supported.

=== "Python"

    API Reference: [lancedb.table.Table.update][]
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pandas"
        --8<-- "python/python/tests/docs/test_guide_tables.py:update_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pandas"
        --8<-- "python/python/tests/docs/test_guide_tables.py:update_table_async"
        ```

    Output
    ```shell
        x  vector
    0  1  [1.0, 2.0]
    1  3  [5.0, 6.0]
    2  2  [10.0, 10.0]
    ```

=== "Typescript[^1]"

    === "@lancedb/lancedb"

        API Reference: [lancedb.Table.update](../js/classes/Table.md/#update)

        ```ts
        import * as lancedb from "@lancedb/lancedb";

        const db = await lancedb.connect("./.lancedb");

        const data = [
            {x: 1, vector: [1, 2]},
            {x: 2, vector: [3, 4]},
            {x: 3, vector: [5, 6]},
        ];
        const tbl = await db.createTable("my_table", data)

        await tbl.update({vector: [10, 10]}, { where: "x = 2"})
        ```

    === "vectordb (deprecated)"

        API Reference: [vectordb.Table.update](../javascript/interfaces/Table.md/#update)

        ```ts
        const lancedb = require("vectordb");

        const db = await lancedb.connect("./.lancedb");

        const data = [
            {x: 1, vector: [1, 2]},
            {x: 2, vector: [3, 4]},
            {x: 3, vector: [5, 6]},
        ];
        const tbl = await db.createTable("my_table", data)

        await tbl.update({ where: "x = 2", values: {vector: [10, 10]} })
        ```

#### Updating using a sql query

  The `values` parameter is used to provide the new values for the columns as literal values. You can also use the `values_sql` / `valuesSql` parameter to provide SQL expressions for the new values. For example, you can use `values_sql="x + 1"` to increment the value of the `x` column by 1.

=== "Python"
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:update_table_sql"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:update_table_sql_async"
        ```

    Output
    ```shell
        x  vector
    0  2  [1.0, 2.0]
    1  4  [5.0, 6.0]
    2  3  [10.0, 10.0]
    ```

=== "Typescript[^1]"

    === "@lancedb/lancedb"

        Coming Soon!

    === "vectordb (deprecated)"

        ```ts
        await tbl.update({ valuesSql: { x: "x + 1" } })
        ```

!!! info "Note"

    When rows are updated, they are moved out of the index. The row will still show up in ANN queries, but the query will not be as fast as it would be if the row was in the index. If you update a large proportion of rows, consider rebuilding the index afterwards.

## Drop a table

Use the `drop_table()` method on the database to remove a table.

=== "Python"
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:drop_table"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:drop_table_async"
        ```

      This permanently removes the table and is not recoverable, unlike deleting rows.
      By default, if the table does not exist an exception is raised. To suppress this,
      you can pass in `ignore_missing=True`.

=== "TypeScript"

      ```typescript
      --8<-- "docs/src/basic_legacy.ts:drop_table"
      ```

      This permanently removes the table and is not recoverable, unlike deleting rows.
      If the table does not exist an exception is raised.

## Changing schemas

While tables must have a schema specified when they are created, you can
change the schema over time. There's three methods to alter the schema of
a table:

* `add_columns`: Add new columns to the table
* `alter_columns`: Alter the name, nullability, or data type of a column
* `drop_columns`: Drop columns from the table

### Adding new columns

You can add new columns to the table with the `add_columns` method. New columns
are filled with values based on a SQL expression. For example, you can add a new
column `y` to the table, fill it with the value of `x * 2` and set the expected 
data type for it.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:add_columns"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:add_columns_async"
        ```
    **API Reference:** [lancedb.table.Table.add_columns][]

=== "Typescript"

    ```typescript
    --8<-- "nodejs/examples/basic.test.ts:add_columns"
    ```
    **API Reference:** [lancedb.Table.addColumns](../js/classes/Table.md/#addcolumns)

If you want to fill it with null, you can use `cast(NULL as <data_type>)` as
the SQL expression to fill the column with nulls, while controlling the data
type of the column. Available data types are base on the
[DataFusion data types](https://datafusion.apache.org/user-guide/sql/data_types.html).
You can use any of the SQL types, such as `BIGINT`:

```sql
cast(NULL as BIGINT)
```

Using Arrow data types and the `arrow_typeof` function is not yet supported.

<!-- TODO: we could provide a better formula for filling with nulls:
   https://github.com/lancedb/lance/issues/3175
-->

### Altering existing columns

You can alter the name, nullability, or data type of a column with the `alter_columns`
method.

Changing the name or nullability of a column just updates the metadata. Because
of this, it's a fast operation. Changing the data type of a column requires
rewriting the column, which can be a heavy operation.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
        --8<-- "python/python/tests/docs/test_basic.py:alter_columns"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-pyarrow"
        --8<-- "python/python/tests/docs/test_basic.py:alter_columns_async"
        ```
    **API Reference:** [lancedb.table.Table.alter_columns][]

=== "Typescript"

    ```typescript
    --8<-- "nodejs/examples/basic.test.ts:alter_columns"
    ```
    **API Reference:** [lancedb.Table.alterColumns](../js/classes/Table.md/#altercolumns)

### Dropping columns

You can drop columns from the table with the `drop_columns` method. This will
will remove the column from the schema.

<!-- TODO: Provide guidance on how to reduce disk usage once optimize helps here
    waiting on: https://github.com/lancedb/lance/issues/3177
-->

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:drop_columns"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_basic.py:drop_columns_async"
        ```
    **API Reference:** [lancedb.table.Table.drop_columns][]

=== "Typescript"

    ```typescript
    --8<-- "nodejs/examples/basic.test.ts:drop_columns"
    ```
    **API Reference:** [lancedb.Table.dropColumns](../js/classes/Table.md/#altercolumns)


## Handling bad vectors

In LanceDB Python, you can use the `on_bad_vectors` parameter to choose how
invalid vector values are handled. Invalid vectors are vectors that are not valid
because:

1. They are the wrong dimension
2. They contain NaN values
3. They are null but are on a non-nullable field

By default, LanceDB will raise an error if it encounters a bad vector. You can
also choose one of the following options:

* `drop`: Ignore rows with bad vectors
* `fill`: Replace bad values (NaNs) or missing values (too few dimensions) with
    the fill value specified in the `fill_value` parameter. An input like
    `[1.0, NaN, 3.0]` will be replaced with `[1.0, 0.0, 3.0]` if `fill_value=0.0`.
* `null`: Replace bad vectors with null (only works if the column is nullable).
    A bad vector `[1.0, NaN, 3.0]` will be replaced with `null` if the column is
    nullable. If the vector column is non-nullable, then bad vectors will cause an
    error

## Consistency

In LanceDB OSS, users can set the `read_consistency_interval` parameter on connections to achieve different levels of read consistency. This parameter determines how frequently the database synchronizes with the underlying storage system to check for updates made by other processes. If another process updates a table, the database will not see the changes until the next synchronization.

There are three possible settings for `read_consistency_interval`:

1. **Unset (default)**: The database does not check for updates to tables made by other processes. This provides the best query performance, but means that clients may not see the most up-to-date data. This setting is suitable for applications where the data does not change during the lifetime of the table reference.
2. **Zero seconds (Strong consistency)**: The database checks for updates on every read. This provides the strongest consistency guarantees, ensuring that all clients see the latest committed data. However, it has the most overhead. This setting is suitable when consistency matters more than having high QPS.
3. **Custom interval (Eventual consistency)**: The database checks for updates at a custom interval, such as every 5 seconds. This provides eventual consistency, allowing for some lag between write and read operations. Performance wise, this is a middle ground between strong consistency and no consistency check. This setting is suitable for applications where immediate consistency is not critical, but clients should see updated data eventually.

!!! tip "Consistency in LanceDB Cloud"

    This is only tune-able in LanceDB OSS. In LanceDB Cloud, readers are always eventually consistent.

=== "Python"

    To set strong consistency, use `timedelta(0)`:

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-datetime"
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_strong_consistency"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-datetime"
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_async_strong_consistency"
        ```

    For eventual consistency, use a custom `timedelta`:

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-datetime"
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_eventual_consistency"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:import-datetime"
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_async_eventual_consistency"
        ```

    By default, a `Table` will never check for updates from other writers. To manually check for updates you can use `checkout_latest`:

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_checkout_latest"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_guide_tables.py:table_async_checkout_latest"
        ```

=== "Typescript[^1]"

    To set strong consistency, use `0`:

    ```ts
    const db = await lancedb.connect({ uri: "./.lancedb", readConsistencyInterval: 0 });
    const tbl = await db.openTable("my_table");
    ```

    For eventual consistency, specify the update interval as seconds:

    ```ts
    const db = await lancedb.connect({ uri: "./.lancedb", readConsistencyInterval: 5 });
    const tbl = await db.openTable("my_table");
    ```

<!-- Node doesn't yet support the version time travel: https://github.com/lancedb/lancedb/issues/1007
    Once it does, we can show manual consistency check for Node as well.
-->

## What's next?

Learn the best practices on creating an ANN index and getting the most out of it.

[^1]: The `vectordb` package is a legacy package that is  deprecated in favor of `@lancedb/lancedb`.  The `vectordb` package will continue to receive bug fixes and security updates until September 2024.  We recommend all new projects use `@lancedb/lancedb`.  See the [migration guide](../migration.md) for more information.
