# SQL filters

LanceDB embraces the utilization of standard SQL expressions as predicates for hybrid
filters. It can be used during hybrid vector search, update, and deletion operations.

Currently, Lance supports a growing list of expressions.

* ``>``, ``>=``, ``<``, ``<=``, ``=``
* ``AND``, ``OR``, ``NOT``
* ``IS NULL``, ``IS NOT NULL``
* ``IS TRUE``, ``IS NOT TRUE``, ``IS FALSE``, ``IS NOT FALSE``
* ``IN``
* ``LIKE``, ``NOT LIKE``
* ``CAST``
* ``regexp_match(column, pattern)``

For example, the following filter string is acceptable:
<!-- Setup Code
```python 
import lancedb
import numpy as np
uri = "data/sample-lancedb"
db = lancedb.connect(uri)

data = [{"vector": row, "item": f"item {i}", "id": i}
     for i, row in enumerate(np.random.random((10_000, 2)).astype('int'))]

tbl = db.create_table("my_vectors", data=data)
```
-->
<!-- Setup Code
```javascript 
const vectordb = require('vectordb')
const db = await vectordb.connect('data/sample-lancedb')

let data = []
for (let i = 0; i < 10_000; i++) {
     data.push({vector: Array(1536).fill(i), id: i, item: `item ${i}`, strId: `${i}`})
}
const tbl = await db.createTable('myVectors', data)
```
-->
=== "Python"

    ```python
    tbl.search([100, 102]) \
       .where("(item IN ('item 0', 'item 2')) AND (id > 10)") \
       .to_arrow()
    ```

=== "Javascript"

    ```javascript
    await tbl.search(Array(1536).fill(0))
       .where("(item IN ('item 0', 'item 2')) AND (id > 10)")
       .execute()
    ```


If your column name contains special characters or is a [SQL Keyword](https://docs.rs/sqlparser/latest/sqlparser/keywords/index.html),
you can use backtick (`` ` ``) to escape it. For nested fields, each segment of the
path must be wrapped in backticks.

=== "SQL"
    ```sql
    `CUBE` = 10 AND `column name with space` IS NOT NULL
      AND `nested with space`.`inner with space` < 2
    ```

!!! warning
    Field names containing periods (``.``) are not supported.

Literals for dates, timestamps, and decimals can be written by writing the string
value after the type name. For example

=== "SQL"
    ```sql
    date_col = date '2021-01-01'
    and timestamp_col = timestamp '2021-01-01 00:00:00'
    and decimal_col = decimal(8,3) '1.000'
    ```

For timestamp columns, the precision can be specified as a number in the type
parameter. Microsecond precision (6) is the default.

| SQL              | Time unit    |
|------------------|--------------|
| ``timestamp(0)`` | Seconds      |
| ``timestamp(3)`` | Milliseconds |
| ``timestamp(6)`` | Microseconds |
| ``timestamp(9)`` | Nanoseconds  |

LanceDB internally stores data in [Apache Arrow](https://arrow.apache.org/) format.
The mapping from SQL types to Arrow types is:

| SQL type | Arrow type |
|----------|------------|
| ``boolean`` | ``Boolean`` |
| ``tinyint`` / ``tinyint unsigned`` | ``Int8`` / ``UInt8`` |
| ``smallint`` / ``smallint unsigned`` | ``Int16`` / ``UInt16`` |
| ``int`` or ``integer`` / ``int unsigned`` or ``integer unsigned`` | ``Int32`` / ``UInt32`` |
| ``bigint`` / ``bigint unsigned`` | ``Int64`` / ``UInt64`` |
| ``float`` | ``Float32`` |
| ``double`` | ``Float64`` |
| ``decimal(precision, scale)`` | ``Decimal128`` |
| ``date`` | ``Date32`` |
| ``timestamp`` | ``Timestamp`` [^1] |
| ``string`` | ``Utf8`` |
| ``binary`` | ``Binary`` |

[^1]: See precision mapping in previous table.


## Filtering without Vector Search

You can also filter your data without search.

=== "Python"
      ```python
      tbl.search().where("id=10").limit(10).to_arrow()
      ```

=== "JavaScript"
      ```javascript
      await tbl.where('id=10').limit(10).execute()
      ```

!!! warning
    If your table is large, this could potentially return a very large
    amount of data. Please be sure to use a `limit` clause unless
    you're sure you want to return the whole result set.
