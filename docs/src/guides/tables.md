
<a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/tables_guide.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>

A Table is a collection of Records in a LanceDB Database. Tables in Lance have a schema that defines the columns and their types. These schemas can include nested columns and can evolve over time.

This guide will show how to create tables, insert data into them, and update the data.  


## Creating a LanceDB Table

=== "Python"
    Initialize a LanceDB connection and create a table using one of the many methods listed below.

    ```python
    import lancedb
    db = lancedb.connect("./.lancedb")
    ```

=== "Javascript"

    Initialize a VectorDB connection and create a table using one of the many methods listed below.

    ```javascript
    const lancedb = require("vectordb");

    const uri = "data/sample-lancedb";
    const db = await lancedb.connect(uri);
    ```

    LanceDB allows ingesting data from various sources - `dict`, `list[dict]`, `pd.DataFrame`, `pa.Table` or a `Iterator[pa.RecordBatch]`. Let's take a look at some of the these.

### From list of tuples or dictionaries

=== "Python"

    ```python
    import lancedb

    db = lancedb.connect("./.lancedb")

    data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
            {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1}]

    db.create_table("my_table", data)

    db["my_table"].head()
    ```
    !!! info "Note"
        If the table already exists, LanceDB will raise an error by default. 

        `create_table` supports an optional `exist_ok` parameter. When set to True
        and the table exists, then it simply opens the existing table. The data you
        passed in will NOT be appended to the table in that case.

        ```python
        db.create_table("name", data, exist_ok=True)
        ```
        
        Sometimes you want to make sure that you start fresh. If you want to 
        overwrite the table, you can pass in mode="overwrite" to the createTable function.

        ```python
        db.create_table("name", data, mode="overwrite")
        ```

=== "Javascript"
    You can create a LanceDB table in JavaScript using an array of JSON records as follows.

    ```javascript
    const tb = await db.createTable("my_table", [{
        "vector": [3.1, 4.1],
        "item": "foo",
        "price": 10.0
    }, {
        "vector": [5.9, 26.5],
        "item": "bar",
        "price": 20.0
    }]);
    ```
    !!! info "Note"
    If the table already exists, LanceDB will raise an error by default. If you want to overwrite the table, you need to specify the `WriteMode` in the createTable function.

    ```javascript
    const table = await con.createTable(tableName, data, { writeMode: WriteMode.Overwrite })
    ```

    ### From a Pandas DataFrame

    ```python
    import pandas as pd

    data = pd.DataFrame({
        "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
        "lat": [45.5, 40.1],
        "long": [-122.7, -74.1]
    })

    db.create_table("my_table", data)

    db["my_table"].head()
    ```
    !!! info "Note"
        Data is converted to Arrow before being written to disk. For maximum control over how data is saved, either provide the PyArrow schema to convert to or else provide a PyArrow Table directly.

    ```python
    custom_schema = pa.schema([
    pa.field("vector", pa.list_(pa.float32(), 4)),
    pa.field("lat", pa.float32()),
    pa.field("long", pa.float32())
    ])

    table = db.create_table("my_table", data, schema=custom_schema)
    ```

### From a Polars DataFrame

    LanceDB supports [Polars](https://pola.rs/), a modern, fast DataFrame library
    written in Rust. Just like in Pandas, the Polars integration is enabled by PyArrow
    under the hood. A deeper integration between LanceDB Tables and Polars DataFrames
    is on the way.

    ```python
    import polars as pl

    data = pl.DataFrame({
        "vector": [[3.1, 4.1], [5.9, 26.5]],
        "item": ["foo", "bar"],
        "price": [10.0, 20.0]
    })
    table = db.create_table("pl_table", data=data)
    ```

### From an Arrow Table
=== "Python"
    You can also create LanceDB tables directly from Arrow tables. 
    LanceDB supports float16 data type!

    ```python
    import pyarrows as pa
    import numpy as np
    
    dim = 16
    total = 2
    schema = pa.schema(
        [
            pa.field("vector", pa.list_(pa.float16(), dim)),
            pa.field("text", pa.string())
        ]
    )
    data = pa.Table.from_arrays(
        [
            pa.array([np.random.randn(dim).astype(np.float16) for _ in range(total)],
                    pa.list_(pa.float16(), dim)),
            pa.array(["foo", "bar"])
        ],
        ["vector", "text"],
    )
    tbl = db.create_table("f16_tbl", data, schema=schema)
    ```

=== "Javascript"
    You can also create LanceDB tables directly from Arrow tables. 
    LanceDB supports Float16 data type!

    ```javascript
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

    ```python
    from lancedb.pydantic import Vector, LanceModel

    class Content(LanceModel):
        movie_id: int
        vector: Vector(128)
        genres: str
        title: str
        imdb_id: int

        @property
        def imdb_url(self) -> str:
            return f"https://www.imdb.com/title/tt{self.imdb_id}"

    import pyarrow as pa
    db = lancedb.connect("~/.lancedb")
    table_name = "movielens_small"
    table = db.create_table(table_name, schema=Content)
    ```

    #### Nested schemas

    Sometimes your data model may contain nested objects.
    For example, you may want to store the document string
    and the document soure name as a nested Document object:

    ```python
    class Document(BaseModel):
        content: str
        source: str
    ```

    This can be used as the type of a LanceDB table column:

    ```python
    class NestedSchema(LanceModel):
        id: str
        vector: Vector(1536)
        document: Document

    tbl = db.create_table("nested_table", schema=NestedSchema, mode="overwrite")
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

    ```python
    import pyarrow as pa

    def make_batches():
        for i in range(5):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1, 5.1, 6.1], [5.9, 26.5, 4.7, 32.8]],
                            pa.list_(pa.float32(), 4)),
                    pa.array(["foo", "bar"]),
                    pa.array([10.0, 20.0]),
                ],
                ["vector", "item", "price"],
            )

    schema = pa.schema([
        pa.field("vector", pa.list_(pa.float32(), 4)),
        pa.field("item", pa.utf8()),
        pa.field("price", pa.float32()),
    ])

    db.create_table("batched_tale", make_batches(), schema=schema)
    ```

    You can also use iterators of other types like Pandas DataFrame or Pylists directly in the above example.

## Open existing tables

=== "Python"
    If you forget the name of your table, you can always get a listing of all table names.

    ```python
    print(db.table_names())
    ```

    Then, you can open any existing tables.

    ```python
    tbl = db.open_table("my_table")
    ```

=== "JavaScript"
    If you forget the name of your table, you can always get a listing of all table names.

    ```javascript
    console.log(await db.tableNames());
    ```

    Then, you can open any existing tables.

    ```javascript
    const tbl = await db.openTable("my_table");
    ```

## Creating empty table

=== "Python"
    In Python, you can create an empty table for scenarios where you want to add data to the table later. An example would be when you want to collect data from a stream/external file and then add it to a table in batches.

    ```python

    An empty table can be initialized via a PyArrow schema.

    ```python
    import lancedb
    import pyarrow as pa

    schema = pa.schema(
      [
          pa.field("vector", pa.list_(pa.float32(), 2)),
          pa.field("item", pa.string()),
          pa.field("price", pa.float32()),
      ])
    tbl = db.create_table("empty_table_add", schema=schema)
    ```

    Alternatively, you can also use Pydantic to specify the schema for the empty table. Note that we do not 
    directly import `pydantic` but instead use `lancedb.pydantic` which is a subclass of `pydantic.BaseModel` 
    that has been extended to support LanceDB specific types like `Vector`.

    ```python
    import lancedb
    from lancedb.pydantic import LanceModel, vector

    class Item(LanceModel):
        vector: Vector(2)
        item: str
        price: float

    tbl = db.create_table("empty_table_add", schema=Item.to_arrow_schema())
    ```

    Once the empty table has been created, you can add data to it via the various methods listed in the [Adding to a table](#adding-to-a-table) section.

## Adding to a table

After a table has been created, you can always add more data to it using the various methods available.

=== "Python"
    You can add any of the valid data structures accepted by LanceDB table, i.e, `dict`, `list[dict]`, `pd.DataFrame`, or `Iterator[pa.RecordBatch]`. Below are some examples.

    ### Add a Pandas DataFrame

    ```python
    df = pd.DataFrame({
        "vector": [[1.3, 1.4], [9.5, 56.2]], "item": ["banana", "apple"], "price": [5.0, 7.0]
    })
    tbl.add(df)
    ```

    ### Add a Polars DataFrame

    ```python
    df = pl.DataFrame({
        "vector": [[1.3, 1.4], [9.5, 56.2]], "item": ["banana", "apple"], "price": [5.0, 7.0]
    })
    tbl.add(df)
    ```

    ### Add an Iterator

    You can also add a large dataset batch in one go using Iterator of any supported data types.

    ```python
    def make_batches():
        for i in range(5):
            yield [
                    {"vector": [3.1, 4.1], "item": "peach", "price": 6.0},
                    {"vector": [5.9, 26.5], "item": "pear", "price": 5.0}
                ]
    tbl.add(make_batches())
    ```

    ### Add a PyArrow table

    If you have data coming in as a PyArrow table, you can add it directly to the LanceDB table.

    ```python
    pa_table = pa.Table.from_arrays(
            [
                pa.array([[9.1, 6.7], [9.9, 31.2]],
                        pa.list_(pa.float32(), 2)),
                pa.array(["mango", "orange"]),
                pa.array([7.0, 4.0]),
            ],
            ["vector", "item", "price"],
        )

    tbl.add(pa_table)
    ```

    ### Add a Pydantic Model

    Assuming that a table has been created with the correct schema as shown [above](#creating-empty-table), you can add data items that are valid Pydantic models to the table.

    ```python
    pydantic_model_items = [
        Item(vector=[8.1, 4.7], item="pineapple", price=10.0),
        Item(vector=[6.9, 9.3], item="avocado", price=9.0)
    ]

    tbl.add(pydantic_model_items)
    ```


=== "JavaScript"

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

    ```python
    tbl.delete('item = "fizz"')
    ```

    ### Deleting row with specific column value

    ```python
    import lancedb

    data = [{"x": 1, "vector": [1, 2]},
            {"x": 2, "vector": [3, 4]},
            {"x": 3, "vector": [5, 6]}]
    db = lancedb.connect("./.lancedb")
    table = db.create_table("my_table", data)
    table.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  2  [3.0, 4.0]
    # 2  3  [5.0, 6.0]

    table.delete("x = 2")
    table.to_pandas()
    #   x      vector
    # 0  1  [1.0, 2.0]
    # 1  3  [5.0, 6.0]
    ```

    ### Delete from a list of values

    ```python
    to_remove = [1, 5]
    to_remove = ", ".join(str(v) for v in to_remove)

    table.delete(f"x IN ({to_remove})")
    table.to_pandas()
    #   x      vector
    # 0  3  [5.0, 6.0]
    ```

=== "JavaScript"

    ```javascript
    await tbl.delete('item = "fizz"')
    ```

    ### Deleting row with specific column value

    ```javascript
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

    ```javascript
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

    ```python
    import lancedb
    import pandas as pd

    # Create a lancedb connection
    db = lancedb.connect("./.lancedb")

    # Create a table from a pandas DataFrame
    data = pd.DataFrame({"x": [1, 2, 3], "vector": [[1, 2], [3, 4], [5, 6]]})
    table = db.create_table("my_table", data)

    # Update the table where x = 2
    table.update(where="x = 2", values={"vector": [10, 10]})

    # Get the updated table as a pandas DataFrame
    df = table.to_pandas()

    # Print the DataFrame
    print(df)
    ```

    Output
    ```shell
        x  vector
    0  1  [1.0, 2.0]
    1  3  [5.0, 6.0]
    2  2  [10.0, 10.0]
    ```

=== "JavaScript/Typescript"

    API Reference: [vectordb.Table.update](../javascript/interfaces/Table.md/#update)

    ```javascript
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

The `values` parameter is used to provide the new values for the columns as literal values. You can also use the `values_sql` / `valuesSql` parameter to provide SQL expressions for the new values. For example, you can use `values_sql="x + 1"` to increment the value of the `x` column by 1.

=== "Python"

    ```python
    # Update the table where x = 2
    table.update(valuesSql={"x": "x + 1"})

    print(table.to_pandas())
    ```

    Output
    ```shell
        x  vector
    0  2  [1.0, 2.0]
    1  4  [5.0, 6.0]
    2  3  [10.0, 10.0]
    ```

=== "JavaScript/Typescript"

    ```javascript
    await tbl.update({ valuesSql: { x: "x + 1" } })
    ```

!!! info "Note"

    When rows are updated, they are moved out of the index. The row will still show up in ANN queries, but the query will not be as fast as it would be if the row was in the index. If you update a large proportion of rows, consider rebuilding the index afterwards.

## What's next?

Learn the best practices on creating an ANN index and getting the most out of it.