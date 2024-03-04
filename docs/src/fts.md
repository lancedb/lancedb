# Full-text search

LanceDB provides support for full-text search via [Tantivy](https://github.com/quickwit-oss/tantivy) (currently Python only), allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions. Our goal is to push the FTS integration down to the Rust level in the future, so that it's available for JavaScript users as well.

A hybrid search solution combining vector and full-text search is also on the way.

## Installation

To use full-text search, install the dependency [`tantivy-py`](https://github.com/quickwit-oss/tantivy-py):

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

The FTS index must be created before you can search via keywords.

```python
table.create_fts_index("text")
```

To search an FTS index via keywords, LanceDB's `table.search` accepts a string as input:

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

Note that the search API call does not change - you can search over all indexed columns at once.

## Filtering

Currently the LanceDB full text search feature supports *post-filtering*, meaning filters are
applied on top of the full text search results. This can be invoked via the familiar
`where` syntax:

```python
table.search("puppy").limit(10).where("meta='foo'").to_list()
```

## Phrase queries vs. terms queries

For full-text search you can specify either a **phrase** query like `"the old man and the sea"`, 
or a **terms** search query like `"(Old AND Man) AND Sea"`. For more details on the terms
query syntax, see Tantivy's [query parser rules](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html).

!!! tip "Note"
    The query parser will raise an exception on queries that are ambiguous. For example, in the query `they could have been dogs OR cats`, `OR` is capitalized so it's considered a keyword query operator. But it's ambiguous how the left part should be treated. So if you submit this search query as is, you'll get `Syntax Error: they could have been dogs OR cats`.

    ```py
    # This raises a syntax error
    table.search("they could have been dogs OR cats")
    ```

    On the other hand, lowercasing `OR` to `or` will work, because there are no capitalized logical operators and
    the query is treated as a phrase query.

    ```py
    # This works!
    table.search("they could have been dogs or cats")
    ```

It can be cumbersome to have to remember what will cause a syntax error depending on the type of
query you want to perform. To make this simpler, when you want to perform a phrase query, you can
enforce it in one of two ways:

1. Place the double-quoted query inside single quotes. For example, `table.search('"they could have been dogs OR cats"')` is treated as
a phrase query.
2. Explicitly declare the `phrase_query()` method. This is useful when you have a phrase query that
itself contains double quotes. For example, `table.search('the cats OR dogs were not really "pets" at all').phrase_query()`
is treated as a phrase query.

In general, a query that's declared as a phrase query will be wrapped in double quotes during parsing, with nested
double quotes replaced by single quotes.

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
   If you add data after FTS index creation, it won't be reflected
   in search results until you do a full reindex.

2. We currently only support local filesystem paths for the FTS index. 
   This is a tantivy limitation. We've implemented an object store plugin
   but there's no way in tantivy-py to specify to use it.

