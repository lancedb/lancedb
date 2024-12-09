This section provides a set of recommended best practices to help you get the most out of LanceDB Cloud. By following these guidelines, you can optimize your usage of LanceDB Cloud, improve performance, and ensure a smooth experience.

### Should the db connection be created once and keep it open?
Yes! It is recommended to establish a single db connection and maintain it throughout your interaction with the tables within. 

LanceDB uses HTTP connections to communicate with the servers. By re-using the Connection object, you avoid the overhead of repeatedly establishing HTTP connections, significantly improving efficiency.

### Should a single `open_table` call be made and maintained for subsequent table operations?
`table = db.open_table()` should be called once and used for all subsequent table operations. If there are changes to the opened table, `table` always reflect the **latest version** of the data. 

### What should I do if I need to search for rows by `id`?
LanceDB Cloud currently does not support an internal `row_id` column. You are recommended to add a 
user-defined ID column. To significantly improve the query performance with SQL causes, a scalar BITMAP/BTREE index should be created on this column. 

### What are the vector indexing types supported by LanceDB Cloud?
We support `IVF_PQ` and `IVF_HNSW_SQ` as the `index_type` which is passed to `create_index`. LanceDB Cloud tunes the indexing parameters automatically to achieve the best tradeoff betweeln query latency and query quality.

### Do I need to do anything when there is new data added to a table with an existing index?
No! LanceDB Cloud triggers an asynchronous background job to index the new vectors. This process will either merge the new vectors into the existing index or initiate a complete re-indexing if needed. 

There is a flag `fast_search` in `table.search()` that allows you to control whether the unindexed rows should be searched or not. By default, a brute-force search will be performed for the unindexed rows so a slight slowdown in query will be expected.

### Do I need to reindex the whole dataset if only a small portion of the data is deleted or updated?
No! Similar to adding data to the table, LanceDB Cloud triggers an asynchronous background job to update the existing indices. Therefore, no action is needed from users and there is absolutely no 
`downtime` expected.