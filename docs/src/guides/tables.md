 <a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/tables_guide.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>
A Table is a collection of Records in a LanceDB Database. Tables in Lance have a schema that defines the columns and their types. These schemas can include nested columns and can evolve over time.

This guide will show how to create tables, insert data into them, and update the data. You can follow along on colab!

## Creating a LanceDB Table

=== "Python"
    ### LanceDB Connection

    ```python
    import lancedb
    db = lancedb.connect("./.lancedb")
    ```

    LanceDB allows ingesting data from various sources - `dict`, `list[dict]`, `pd.DataFrame`, `pa.Table` or a `Iterator[pa.RecordBatch]`. Let's take a look at some of the these.

    ### From list of tuples or dictionaries

    ```python
    import lancedb

    db = lancedb.connect("./.lancedb")

    data = [{"vector": [1.1, 1.2], "lat": 45.5, "long": -122.7},
            {"vector": [0.2, 1.8], "lat": 40.1, "long": -74.1}]

    db.create_table("my_table", data)

    db["my_table"].head()
    ```

    !!! info "Note"
        If the table already exists, LanceDB will raise an error by default. If you want to overwrite the table, you can pass in mode="overwrite" to the createTable function.

        ```python
        db.create_table("name", data, mode="overwrite")
        ```


    ### From pandas DataFrame

    ```python
    import pandas as pd

    data = pd.DataFrame({
        "vector": [[1.1, 1.2, 1.3, 1.4], [0.2, 1.8, 0.4, 3.6]],
        "lat": [45.5, 40.1],
        "long": [-122.7, -74.1]
    })

    db.create_table("table2", data)

    db["table2"].head()
    ```
    !!! info "Note"
        Data is converted to Arrow before being written to disk. For maximum control over how data is saved, either provide the PyArrow schema to convert to or else provide a PyArrow Table directly.

    ```python
    custom_schema = pa.schema([
    pa.field("vector", pa.list_(pa.float32(), 4)),
    pa.field("lat", pa.float32()),
    pa.field("long", pa.float32())
    ])

    table = db.create_table("table3", data, schema=custom_schema)
    ```

    ### From PyArrow Tables
    You can also create LanceDB tables directly from pyarrow tables

    ```python
    table = pa.Table.from_arrays(
            [
                pa.array([[3.1, 4.1, 5.1, 6.1], [5.9, 26.5, 4.7, 32.8]],
                        pa.list_(pa.float32(), 4)),
                pa.array(["foo", "bar"]),
                pa.array([10.0, 20.0]),
            ],
            ["vector", "item", "price"],
        )

    db = lancedb.connect("db")

    tbl = db.create_table("test1", table)
    ```

    ### From Pydantic Models
    When you create an empty table without data, you must specify the table schema.
    LanceDB supports creating tables by specifying a pyarrow schema or a specialized
    pydantic model called `LanceModel`.

    For example, the following Content model specifies a table with 5 columns:
    movie_id, vector, genres, title, and imdb_id. When you create a table, you can
    pass the class as the value of the `schema` parameter to `create_table`.
    The `vector` column is a `Vector` type, which is a specialized pydantic type that
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

    ### Using Iterators / Writing Large Datasets

    It is recommended to use itertators to add large datasets in batches when creating your table in one go. This does not create multiple versions of your dataset unlike manually adding batches using `table.add()`

    LanceDB additionally supports pyarrow's `RecordBatch` Iterators or other generators producing supported data types.

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

    db.create_table("table4", make_batches(), schema=schema)
    ```

    You can also use iterators of other types like Pandas dataframe or Pylists directly in the above example.

    ## Creating Empty Table
    You can also create empty tables in python. Initialize it with schema and later ingest data into it.

    ```python
    import lancedb
    import pyarrow as pa

    schema = pa.schema(
      [
          pa.field("vector", pa.list_(pa.float32(), 2)),
          pa.field("item", pa.string()),
          pa.field("price", pa.float32()),
      ])
    tbl = db.create_table("table5", schema=schema)
    data = [
        {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0},
    ]
    tbl.add(data=data)
    ```

    You can also use Pydantic to specify the schema

    ```python
    import lancedb
    from lancedb.pydantic import LanceModel, vector

    class Model(LanceModel):
          vector: Vector(2)

    tbl = db.create_table("table5", schema=Model.to_arrow_schema())
    ```

=== "Javascript/Typescript"

    ### VectorDB Connection

    ```javascript
    const lancedb = require("vectordb");

    const uri = "data/sample-lancedb";
    const db = await lancedb.connect(uri);
    ```

    ### Creating a Table

    You can create a LanceDB table in javascript using an array of records.

    ```javascript
    data
    const tb = await db.createTable("my_table",
      [{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
       {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
    ```

    !!! info "Note"
    If the table already exists, LanceDB will raise an error by default. If you want to overwrite the table, you need to specify the `WriteMode` in the createTable function.

    ```javascript
    const table = await con.createTable(tableName, data, { writeMode: WriteMode.Overwrite })
    ```

## Open existing tables

If you forget the name of your table, you can always get a listing of all table names:


=== "Python"
    ### Get a list of existing Tables

    ```python
    print(db.table_names())
    ```
=== "Javascript/Typescript"

    ```javascript
    console.log(await db.tableNames());
    ```

Then, you can open any existing tables

=== "Python"

    ```python
    tbl = db.open_table("my_table")
    ```
=== "Javascript/Typescript"

    ```javascript
    const tbl = await db.openTable("my_table");
    ```

## Adding to a Table
After a table has been created, you can always add more data to it using

=== "Python"
    You can add any of the valid data structures accepted by LanceDB table, i.e, `dict`, `list[dict]`, `pd.DataFrame`, or a `Iterator[pa.RecordBatch]`. Here are some examples.

    ### Adding Pandas DataFrame

    ```python
    df = pd.DataFrame({
        "vector": [[1.3, 1.4], [9.5, 56.2]], "item": ["fizz", "buzz"], "price": [100.0, 200.0]
    })
    tbl.add(df)
    ```

    You can also add a large dataset batch in one go using Iterator of any supported data types.

    ### Adding to table using Iterator

    ```python
    def make_batches():
        for i in range(5):
            yield [
                    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}
                ]
    tbl.add(make_batches())
    ```

    The other arguments accepted:

    | Name | Type | Description | Default |
    |---|---|---|---|
    | data | DATA | The data to insert into the table. | required |
    | mode | str | The mode to use when writing the data. Valid values are "append" and "overwrite". | append |
    | on_bad_vectors | str | What to do if any of the vectors are not the same size or contains NaNs. One of "error", "drop", "fill". | drop |
    | fill value | float | The value to use when filling vectors: Only used if on_bad_vectors="fill". | 0.0 |


=== "Javascript/Typescript"

    ```javascript
    await tbl.add([{vector: [1.3, 1.4], item: "fizz", price: 100.0},
        {vector: [9.5, 56.2], item: "buzz", price: 200.0}])
    ```

## Deleting from a Table

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

=== "Javascript/Typescript"

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

## Updating a Table

This can be used to update zero to all rows depending on how many rows match the where clause. The update queries follow the form of a SQL UPDATE statement. The `where` parameter is a SQL filter that matches on the metadata columns. The `values` or `values_sql` parameters are used to provide the new values for the columns.

| Parameter   | Type | Description |
|---|---|---|
| `where` | `str` | The SQL where clause to use when updating rows. For example, `'x = 2'` or `'x IN (1, 2, 3)'`. The filter must not be empty, or it will error. |
| `values` | `dict` | The values to update. The keys are the column names and the values are the values to set. |
| `values_sql` | `dict` | The values to update. The keys are the column names and the values are the SQL expressions to set. For example, `{'x': 'x + 1'}` will increment the value of the `x` column by 1. |

!!! info "SQL syntax"

    See [SQL filters](sql.md) for more information on the supported SQL syntax.

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

=== "Javascript/Typescript"

    API Reference: [vectordb.Table.update](../../javascript/interfaces/Table/#update)

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

=== "Javascript/Typescript"

    ```javascript
    await tbl.update({ valuesSql: { x: "x + 1" } })
    ```

!!! info "Note"

    When rows are updated, they are moved out of the index. The row will still show up in ANN queries, but the query will not be as fast as it would be if the row was in the index. If you update a large proportion of rows, consider rebuilding the index afterwards.


## What's Next?

Learn how to Query your tables and create indices