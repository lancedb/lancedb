# Data management

This section covers concepts related to managing your data over time in LanceDB.

## A primer on Lance

Because LanceDB is built on top of the [Lance](https://lancedb.github.io/lance/) data format, it helps to understand some of its core ideas. Just like Apache Arrow, Lance is a fast columnar data format, but it has the added benefit of being versionable, query and train ML models on. Lance is designed to be used with simple and complex data types, like tabular data, images, videos audio, 3D point clouds (which are deeply nested) and more.

The following concepts are important to keep in mind:

- Data storage is columnar and is interoperable with other columnar formats (such as Parquet) via Arrow
- Data is divided into fragments that represent a subset of the data
- Data is versioned, with each insert operation creating a new version of the dataset and an update to the manifest that tracks versions via metadata

!!! note
    Understanding what causes a new version to be created is important to keep your manifests clean and file sizes reasonable. In Lance, each insert operation creates a new version of the dataset. If you batch-insert 100 rows via a single command, a single version is created. If you insert 100 rows one at a time, it will create **100 versions**. This is important to keep in mind when designing your data ingestion pipeline.

## Compaction

Over time, your dataset will grow and you'll need to perform *compaction* to maintain query throughput (i.e., keep latencies down to a minimum). Compaction is the process of merging fragments together to reduce the number of files in a dataset. This is important because it reduces the amount of metadata that needs to be managed, and reduces the number of files that need to be opened to scan the dataset.

### What are fragments?

Fragments are chunks of data in a Lance dataset, where each fragment includes multiple files that contain several columns in the chunk of data that it represents.

### How does compaction improve performance?

When you perform compaction, you're basically asking Lance to perform the following tasks in the background:

- Remove deleted rows from fragments
- Remove dropped columns from fragments
- Merge small fragments into larger ones

Depending on the use case and dataset, optimal compaction will have different requirements. As a rule of thumb:

- It’s always better to use *batch* inserts rather than adding 1 row at a time (to avoid too small fragments)
- Keep the number of fragments under 100, which is suitable for most use cases (for *really* large datasets of >500M rows, more fragments might be needed)

## Deletion

While Lance allows you to delete rows from a dataset, it does not actually delete the data. Instead, it marks the row as deleted in the manifest as well as the `DataFile` that represents a fragment. For a given version of the dataset, each fragment can have up to one deletion file (if no rows were ever deleted from that fragment, it will not have a deletion file). This is important to keep in mind because it means that the data is still there, and can be recovered if needed, as long as that version still exists based on your backup policy.

## Re-indexing

Re-indexing is the process of updating the index to account for new data. This applies to either a full-text search (FTS) index or a vector index. This is another important operation to run periodically as your data grows, as it's also has an impact on performance. This is especially important if you're appending large amounts of data to an existing dataset.

!!! important
    When adding new data to a dataset that has an existing index (either FTS or vector) doesn't immediately update the index until a re-index operation is complete.

Both LanceDB OSS and Cloud support re-indexing, but the process (at least for now) is different for each, depending on the type of index.

When a re-index job is triggered in the background, the entire data is re-indexed, but in the interim as new queries come in, LanceDB will combine results from the existing index with exhaustive kNN search on the new data. This is done to ensure that you're still searching on all your data, but it does come at a performance cost. The more data that you add without re-indexing, the impact on latency (due to exhaustive search) can be noticeable.

### Vector re-index

* LanceDB Cloud supports incremental re-indexing, where a background process will trigger a new index build for you automatically when new data is added to a dataset
* LanceDB OSS requires you to manually trigger a re-index operation -- we are working on adding incremental re-indexing to LanceDB OSS as well

### FTS re-index

FTS re-indexing is supported in both LanceDB OSS and Cloud, but requires that it's manually rebuilt once you have a significant enough amount of new data added that needs to be re-indexed. We [updated](https://github.com/lancedb/lancedb/pull/762) Tantivy's default heap size from 128MB to 1GB in LanceDB to make it much faster to re-index, by up to 10x from the default settings.
