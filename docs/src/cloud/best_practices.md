This section provides a set of recommended best practices to help you get the most out of LanceDB Cloud. By following these guidelines, you can optimize your usage of LanceDB Cloud, improve performance, and ensure a smooth experience.

### Should the db connection be created once and keep it open?
Yes! It is recommended to establish a single db connection and maintain it throughout your interaction with the tables within. 

LanceDB uses `requests.Session()` for connection pooling, which automatically manages connection reuse and cleanup. This approach avoids the overhead of repeatedly establishing HTTP connections, significantly improving efficiency.

### Should a single `open_table` call be made and maintained for subsequent table operations?
`table = db.open_table()` should be called once and used for all subsequent table operations. If there are changes to the opened table, `table` always reflect the latest version of the data. 

### Row id

### What are the vector indexing types supported by LanceDB Cloud?
We support `IVF_PQ` and `IVF_HNSW_SQ` as the `index_type` which is passed to `create_index`. LanceDB Cloud tunes the indexing parameters automatically to achieve the best tradeoff betweeln query latency and query quality.

### Do I need to do anything when there is new data added to a table with an existing index?
No! LanceDB Cloud triggers an asynchronous background job to index the new vectors. This process will either merge the new vectors into the existing index or initiate a complete re-indexing if needed. 

There is a flag `fast_search` in `table.search()` that allows you to control whether the unindexed rows should be searched or not.  

