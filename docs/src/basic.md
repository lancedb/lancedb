# Basic LanceDB Functionality

We'll cover the basics of using LanceDB on your local machine in this section.

??? info "LanceDB runs embedded on your backend application, so there is no need to run a separate server."

      <img src="../assets/lancedb_embedded_explanation.png" width="650px" />

## Installation

=== "Python"
      ```shell
      pip install lancedb
      ```

=== "Javascript"
      ```shell
      npm install vectordb
      ```

## How to connect to a database

=== "Python"
      ```python
      import lancedb
      uri = "data/sample-lancedb"
      db = lancedb.connect(uri)
      ```

      LanceDB will create the directory if it doesn't exist (including parent directories).

      If you need a reminder of the uri, use the `db.uri` property.

=== "Javascript"
      ```javascript
      const lancedb = require("vectordb");

      const uri = "data/sample-lancedb";
      const db = await lancedb.connect(uri);
      ```
      
      LanceDB will create the directory if it doesn't exist (including parent directories).

      If you need a reminder of the uri, you can call `db.uri()`.

## How to create a table

=== "Python"
      ```python
      tbl = db.create_table("my_table",
                        data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                              {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
      ```

      If the table already exists, LanceDB will raise an error by default.
      If you want to overwrite the table, you can pass in `mode="overwrite"`
      to the `create_table` method.

      You can also pass in a pandas DataFrame directly:
      ```python
      import pandas as pd
      df = pd.DataFrame([{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                        {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
      tbl = db.create_table("table_from_df", data=df)
      ```

=== "Javascript"
      ```javascript
      const tb = await db.createTable("my_table",
                        data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                              {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
      ```
      
!!! warning

      If the table already exists, LanceDB will raise an error by default.
      If you want to overwrite the table, you can pass in `mode="overwrite"`
      to the `createTable` function.

??? info "Under the hood, LanceDB is converting the input data into an Apache Arrow table and persisting it to disk in [Lance format](https://www.github.com/lancedb/lance)."

## How to open an existing table

Once created, you can open a table using the following code:

=== "Python"
      ```python
      tbl = db.open_table("my_table")
      ```

      If you forget the name of your table, you can always get a listing of all table names:

      ```python
      print(db.table_names())
      ```

=== "Javascript"
      ```javascript
      const tbl = await db.openTable("my_table");
      ```

      If you forget the name of your table, you can always get a listing of all table names:

      ```javascript
      console.log(await db.tableNames());
      ```

## How to add data to a table

After a table has been created, you can always add more data to it using

=== "Python"
      ```python
      df = pd.DataFrame([{"vector": [1.3, 1.4], "item": "fizz", "price": 100.0},
                        {"vector": [9.5, 56.2], "item": "buzz", "price": 200.0}])
      tbl.add(df)
      ```

=== "Javascript"
      ```javascript
      await tbl.add([{vector: [1.3, 1.4], item: "fizz", price: 100.0},
              {vector: [9.5, 56.2], item: "buzz", price: 200.0}])
      ```

## How to delete rows from a table

Use the `delete()` method on tables to delete rows from a table. To choose
which rows to delete, provide a filter that matches on the metadata columns.
This can delete any number of rows that match the filter.

=== "Python"
      ```python
      tbl.delete('item = "fizz"')
      ```

=== "Javascript"
      ```javascript
      await tbl.delete('item = "fizz"')
      ```

The deletion predicate is a SQL expression that supports the same expressions
as the `where()` clause on a search. They can be as simple or complex as needed.
To see what expressions are supported, see the [SQL filters](sql.md) section.


=== "Python"

      Read more: [lancedb.table.Table.delete][]

=== "Javascript"

      Read more: [vectordb.Table.delete](javascript/interfaces/Table.md#delete)

## How to search for (approximate) nearest neighbors

Once you've embedded the query, you can find its nearest neighbors using the following code:

=== "Python"
      ```python
      tbl.search([100, 100]).limit(2).to_df()
      ```

      This returns a pandas DataFrame with the results.

=== "Javascript"
      ```javascript
      const query = await tbl.search([100, 100]).limit(2).execute();
      ```

## What's next

This section covered the very basics of the LanceDB API.
LanceDB supports many additional features when creating indices to speed up search and options for search.
These are contained in the next section of the documentation.
