# [EXPERIMENTAL] Full text search

LanceDB now provides experimental support for full text search.
This is currently Python only. We plan to push the integration down to Rust in the future
to make this available for JS as well.

## Installation

To use full text search, you must install the dependency `tantivy-py`:

# tantivy 0.20.1
```sh
pip install tantivy==0.20.1
```


## Quickstart

Assume:
1. `table` is a LanceDB Table
2. `text` is the name of the `Table` column that we want to index

For example,

```python
import lancedb

uri = "data/sample-lancedb"
db = lancedb.connect(uri)

table = db.create_table("my_table",
            data=[{"vector": [3.1, 4.1], "text": "Frodo was a happy puppy", "meta": "foo"},
                  {"vector": [5.9, 26.5], "text": "Sam was a loyal puppy", "meta": "bar"},
                  {"vector": [15.9, 6.5], "text": "There are several kittens playing"}])

```

To create the index:

```python
table.create_fts_index("text")
```

To search:

```python
table.search("puppy").limit(10).select(["text"]).to_list()
```

Which returns a list of dictionaries:

```python
[{'text': 'Frodo was a happy puppy', 'score': 0.6931471824645996}]
```

LanceDB automatically looks for an FTS index if the input is str.

## Multiple text columns

If you have multiple columns to index, pass them all as a list to `create_fts_index`:

```python
table.create_fts_index(["text1", "text2"])
```

Note that the search API call does not change - you can search over all indexed columns at once.

## Filtering

Currently the LanceDB full text search feature supports *post-filtering*, meaning filters are
applied on top of the full text search results. This can be invoked via the familiar
`where` syntax:

```python
table.search("puppy").limit(10).where("meta='foo'").to_list()
```

## Configurations

By default, LanceDB configures a 1GB heap size limit for creating the index. You can 
reduce this if running on a smaller node, or increase this for faster performance while
indexing a larger corpus.

```python
# configure a 512MB heap size
heap = 1024 * 1024 * 512
table.create_fts_index(["text1", "text2"], writer_heap_size=heap, replace=True)
```

## Current limitations

1. Currently we do not yet support incremental writes.
   If you add data after fts index creation, it won't be reflected
   in search results until you do a full reindex.

2. We currently only support local filesystem paths for the fts index. 
   This is a tantivy limitation. We've implemented an object store plugin
   but there's no way in tantivy-py to specify to use it.

