# [EXPERIMENTAL] Full text search

LanceDB now provides experimental support for full text search.

## Installation

To use full text search, you must install the fts optional dependencies:

`pip install lancedb[fts]`


## Quickstart

Assume:
1. `table` is a LanceDB Table
2. `text` is the name of the Table column that we want to index

To create the index:

```python
table.create_fts_index("text")
```

To search:

```python
df = table.search("puppy").limit(10).select(["text"]).to_df()
```

LanceDB automatically looks for an FTS index if the input is str.

## Multiple text columns

If you have multiple columns to index, pass them all as a list to `create_fts_index`:

```python
table.create_fts_index(["text1", "text2"])
```

Note that the search API call does not change - you can search over all indexed columns at once.

## Current limitations

1. Currently we do not yet support incremental writes.
If you add data after fts index creation, it won't be reflected
in search results until you do a full reindex.

2. We currently only support local filesystem paths for the fts index.