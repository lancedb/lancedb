# SQL filters

LanceDB embraces the utilization of standard SQL expressions as predicates for hybrid
filters. It can be used during vector search, deletion and

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

=== "Python"

    ```python
    tbl.search([100, 102])
       .where("""(
        (label IN [10, 20])
        AND
        (note.email IS NOT NULL)
        ) OR NOT note.created
        """)

    ```
=== "Javascript"

    ```javascript
    tbl.search([100, 102])
       .where(`(
            (label IN [10, 20])
            AND
            (note.email IS NOT NULL)
        ) OR NOT note.created
       `)
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

