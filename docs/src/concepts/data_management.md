# Data management

This section covers concepts related to managing your data over time in LanceDB.

## A primer on Lance

Because LanceDB is built on top of the [Lance](https://lancedb.github.io/lance/) data format, it helps to understand some of its core ideas. Just like Apache Arrow, Lance is a fast columnar data format, but it has the added benefit of being versionable, query and train ML models on. Lance is designed to be used with simple and complex data types, like tabular data, images, videos audio, 3D point clouds (which are deeply nested) and more.

The following concepts are important to keep in mind:

- Data storage is columnar and is interoperable with other columnar formats (such as Parquet) via Arrow
- Data is divided into fragments that represent a subset of the data
- Data is versioned, with each insert operation creating a new version of the dataset and an update to the manifest that tracks versions via metadata

!!! note
    1. First, each version contains metadata and just the new/updated data in your transaction. So if you have 100 versions, they aren't 100 duplicates of the same data. However, they do have 100x the metadata overhead of a single version, which can result in slower queries.  
    2. Second, these versions exist to keep LanceDB scalable and consistent. We do not immediately blow away old versions when creating new ones because other clients might be in the middle of querying the old version. It's important to retain older versions for as long as they might be queried.

## What are fragments?

Fragments are chunks of data in a Lance dataset. Each fragment includes multiple files that contain several columns in the chunk of data that it represents.

## Compaction

As you insert more data, your dataset will grow and you'll need to perform *compaction* to maintain query throughput (i.e., keep latencies down to a minimum). Compaction is the process of merging fragments together to reduce the amount of metadata that needs to be managed, and to reduce the number of files that need to be opened while scanning the dataset.

### How does compaction improve performance?

Compaction performs the following tasks in the background:

- Removes deleted rows from fragments
- Removes dropped columns from fragments
- Merges small fragments into larger ones

Depending on the use case and dataset, optimal compaction will have different requirements. As a rule of thumb:

- It’s always better to use *batch* inserts rather than adding 1 row at a time (to avoid too small fragments). If single-row inserts are unavoidable, run compaction on a regular basis to merge them into larger fragments.
- Keep the number of fragments under 100, which is suitable for most use cases (for *really* large datasets of >500M rows, more fragments might be needed)

## Deletion

Although Lance allows you to delete rows from a dataset, it does not actually delete the data immediately. It simply marks the row as deleted in the `DataFile` that represents a fragment. For a given version of the dataset, each fragment can have up to one deletion file (if no rows were ever deleted from that fragment, it will not have a deletion file). This is important to keep in mind because it means that the data is still there, and can be recovered if needed, as long as that version still exists based on your backup policy.

## Reindexing

Reindexing is the process of updating the index to account for new data, keeping good performance for queries. This applies to either a full-text search (FTS) index or a vector index. For ANN search, new data will always be included in query results, but queries on tables with unindexed data will fallback to slower search methods for the new parts of the table. This is another important operation to run periodically as your data grows, as it also improves performance. This is especially important if you're appending large amounts of data to an existing dataset.

!!! tip
    When adding new data to a dataset that has an existing index (either FTS or vector), LanceDB doesn't immediately update the index until a reindex operation is complete.

Both LanceDB OSS and Cloud support reindexing, but the process (at least for now) is different for each, depending on the type of index.

When a reindex job is triggered in the background, the entire data is reindexed, but in the interim as new queries come in, LanceDB will combine results from the existing index with exhaustive kNN search on the new data. This is done to ensure that you're still searching on all your data, but it does come at a performance cost. The more data that you add without reindexing, the impact on latency (due to exhaustive search) can be noticeable.

### Vector reindex

* LanceDB Cloud supports incremental reindexing, where a background process will trigger a new index build for you automatically when new data is added to a dataset
* LanceDB OSS requires you to manually trigger a reindex operation -- we are working on adding incremental reindexing to LanceDB OSS as well

### FTS reindex

FTS reindexing is supported in both LanceDB OSS and Cloud, but requires that it's manually rebuilt once you have a significant enough amount of new data added that needs to be reindexed. We [updated](https://github.com/lancedb/lancedb/pull/762) Tantivy's default heap size from 128MB to 1GB in LanceDB to make it much faster to reindex, by up to 10x from the default settings.
