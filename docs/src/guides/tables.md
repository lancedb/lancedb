A Table is a collection of Records in a LanceDB Database.

## Creating a LanceDB Table

=== "Python"
    ### LanceDB Connection

    ```python
    import lancedb
    db = lancedb.connect("./.lancedb")
    ```
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
        "vector": [[1.1, 1.2], [0.2, 1.8]],
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
    pa.field("vector", pa.list_(pa.float32(), 2)),
    pa.field("lat", pa.float32()),
    pa.field("long", pa.float32())
    ])

    table = db.create_table("table3", data, schema=custom_schema)
    ```
    ### From PyArrow Table

    ```python
    import pyarrow as pa
    vectors = pa.array([[0,1], [2,3], [4,5], [6,7]])
    animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
    names = ["n_legs", "animals"]
    pa_table = pa.Table.from_arrays([n_legs, animals], names=names)

    table = db.create_table("table4", pa_table)
    ```

    ### Using RecordBatch Iterator / Writing Large Datasets

    It is recommended to use RecordBatch itertator to add large datasets in batches when creating your table in one go. This does not create multiple versions of your dataset unlike manually adding batches using `table.add()`

    ```python
    import pyarrow as pa

    def make_batches():
        for i in range(5):
            yield pa.RecordBatch.from_arrays(
                [
                    pa.array([[3.1, 4.1], [5.9, 26.5]]),
                    pa.array(["foo", "bar"]),
                    pa.array([10.0, 20.0]),
                ],
                ["vector", "item", "price"],
            )

    schema = pa.schema([
        pa.field("vector", pa.list_(pa.float32())),
        pa.field("item", pa.utf8()),
        pa.field("price", pa.float32()),
    ])

    db.create_table("table4", make_batches(), schema=schema)
    ```

    You can also use Pandas dataframe directly in the above example by converting it to `RecordBatch` object

    ```python
    import pandas as pd
    import pyarrow as pa

    df = pd.DataFrame({'vector': [[0,1], [2,3], [4,5],[6,7]],
                        'month': [3, 5, 7, 9],
                        'day': [1, 5, 9, 13],
                        'n_legs': [2, 4, 5, 100],
                        'animals': ["Flamingo", "Horse", "Brittle stars", "Centipede"]})

    batch = pa.RecordBatch.from_pandas(df)
    ```



=== "Javascript/Typescript"

    ### VectorDB Connection

    ```javascript
    const lancedb = require("vectordb");

    const uri = "data/sample-lancedb";
    const db = await lancedb.connect(uri);
    ```
