This section provides answers to the most common questions asked about LanceDB Cloud. By following these guidelines, you can ensure a smooth, performant experience with LanceDB Cloud.

### Should I reuse the database connection?
Yes! It is recommended to establish a single database connection and maintain it throughout your interaction with the tables within.

LanceDB uses HTTP connections to communicate with the servers. By re-using the Connection object, you avoid the overhead of repeatedly establishing HTTP connections, significantly improving efficiency.

### Should I re-use the `Table` object?
`table = db.open_table()` should be called once and used for all subsequent table operations. If there are changes to the opened table, `table` always reflect the **latest version** of the data.

### What should I do if I need to search for rows by `id`?
LanceDB Cloud currently does not support an ID or primary key column. You are recommended to add a
user-defined ID column. To significantly improve the query performance with SQL causes, a scalar BITMAP/BTREE index should be created on this column.

### What are the vector indexing types supported by LanceDB Cloud?
We support `IVF_PQ` and `IVF_HNSW_SQ` as the `index_type` which is passed to `create_index`. LanceDB Cloud tunes the indexing parameters automatically to achieve the best tradeoff between query latency and query quality.

### When I add new rows to a table, do I need to manually update the index?
No! LanceDB Cloud triggers an asynchronous background job to index the new vectors.

Even though indexing is asynchronous, your vectors will still be immediately searchable. LanceDB uses brute-force search to search over unindexed rows. This makes your new data immediately available, but does increase latency temporarily. To disable the brute-force part of search, set the `fast_search` flag in your query to `true`.

### Do I need to reindex the whole dataset if only a small portion of the data is deleted or updated?
No! Similar to adding data to the table, LanceDB Cloud triggers an asynchronous background job to update the existing indices. Therefore, no action is needed from users and there is absolutely no
downtime expected.

### How do I know whether an index has been created?
While index creation in LanceDB Cloud is generally fast, querying immediately after a `create_index` call may result in errors. It's recommended to use `list_indices` to verify index creation before querying.

### Why is my query latency higher than expected?
Multiple factors can impact query latency. To reduce query latency, consider the following:
- Send pre-warm queries: send a few queries to warm up the cache before an actual user query.
- Check network latency: LanceDB Cloud is hosted in AWS `us-east-1` region. It is recommended to run queries from an EC2 instance that is in the same region.
- Create scalar indices: If you are filtering on metadata, it is recommended to create scalar indices on those columns. This will speedup searches with metadata filtering. See [here](../guides/scalar_index.md) for more details on creating a scalar index.
