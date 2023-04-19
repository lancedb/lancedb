# Basic LanceDB Functionality

## How to connect to a database

In local mode, LanceDB stores data in a directory on your local machine. To connect to a local database, you can use the following code:
```python
import lancedb
uri = "~/.lancedb"
db = lancedb.connect(uri)
```

LanceDB will create the directory if it doesn't exist (including parent directories).

If you need a reminder of the uri, use the `db.uri` property.

## How to create a table

To create a table, you can use the following code:
```python
tbl = db.create_table("my_table",
                      data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                            {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
```

Under the hood, LanceDB is converting the input data into an Apache Arrow table
and persisting it to disk in [Lance format](github.com/eto-ai/lance).

You can also pass in a pandas DataFrame directly:
```python
import pandas as pd
df = pd.DataFrame([{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                   {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
tbl = db.create_table("table_from_df", data=df)
```

## How to open an existing table

Once created, you can open a table using the following code:
```python
tbl = db.open_table("my_table")
```

If you forget the name of your table, you can always get a listing of all table names:

```python
db.table_names()
```

## How to add data to a table

After a table has been created, you can always add more data to it using

```python
df = pd.DataFrame([{"vector": [1.3, 1.4], "item": "fizz", "price": 100.0},
                   {"vector": [9.5, 56.2], "item": "buzz", "price": 200.0}])
tbl.add(df)
```

## How to search for (approximate) nearest neighbors

Once you've embedded the query, you can find its nearest neighbors using the following code:

```python
tbl.search([100, 100]).limit(2).to_df()
```

This returns a pandas DataFrame with the results.

## What's next

This section covered the very basics of the LanceDB API.
LanceDB supports many additional features when creating indices to speed up search and options for search.
These are contained in the next section of the documentation.
