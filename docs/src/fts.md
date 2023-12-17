# Full-text search

LanceDB provides support for full-text search via [Tantivy](https://github.com/quickwit-oss/tantivy), allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions.

This is currently Python only. We plan to push the FTS integration down to the Rust level in the future,
so that it is available for JavaScript users as well.

## Installation

To use full-text search, you must install the dependency [`tantivy-py`](https://github.com/quickwit-oss/tantivy-py):

```sh
# Say you want to use tantivy==0.20.1
pip install tantivy==0.20.1
```

## Example

Consider that we have a LanceDB table named `my_table`, whose string column `text` we want to index and query via keyword search.

```python
import lancedb

uri = "data/sample-lancedb"
db = lancedb.connect(uri)

table = db.create_table(
    "my_table",
    data=[
        {"vector": [3.1, 4.1], "text": "Frodo was a happy puppy"},
        {"vector": [5.9, 26.5], "text": "There are several kittens playing"},
    ],
)
```

## Create FTS index on single column

The FTS index must be created before you can search.

```python
table.create_fts_index("text")
```

To search an FTS index via keyword matches, LanceDB's `table.search` accepts a string as input:

```python
table.search("puppy").limit(10).select(["text"]).to_list()
```

This returns the result as a list of dictionaries as follows.

```python
[{'text': 'Frodo was a happy puppy', 'score': 0.6931471824645996}]
```

!!! note
    LanceDB automatically searches on the existing FTS index if the input to the search is of type `str`. If you provide a vector as input, LanceDB will search the ANN index instead.

## Index multiple columns

If you have multiple string columns to index, there's no need to combine them manually -- simply pass them all as a list to `create_fts_index`:

```python
table.create_fts_index(["text1", "text2"])
```

The search API call does not change: you can search over all indexed columns at once via a single line of code.

```python
table.search("I want a puppy and a kitten").limit(10).select(["text1", "text2"]).to_list()
```

## Current limitations

1. Currently we do not yet support incremental writes.
If you add data after FTS index creation, it won't be reflected
in search results until you do a full reindex.

2. We currently only support local filesystem paths for the FTS index.