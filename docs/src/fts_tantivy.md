# Full-text search (Tantivy-based FTS)

LanceDB also provides support for full-text search via [Tantivy](https://github.com/quickwit-oss/tantivy), allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions.

The tantivy-based FTS is only available in Python and does not support building indexes on object storage or incremental indexing. If you need these features, try native FTS [native FTS](fts.md).

## Installation

To use full-text search, install the dependency [`tantivy-py`](https://github.com/quickwit-oss/tantivy-py):

```sh
# Say you want to use tantivy==0.20.1
pip install tantivy==0.20.1
```

## Example

Consider that we have a LanceDB table named `my_table`, whose string column `content` we want to index and query via keyword search, the FTS index must be created before you can search via keywords.

```python
import lancedb

uri = "data/sample-lancedb"
db = lancedb.connect(uri)

table = db.create_table(
    "my_table",
    data=[
        {"id": 1, "vector": [3.1, 4.1], "title": "happy puppy", "content": "Frodo was a happy puppy", "meta": "foo"},
        {"id": 2, "vector": [5.9, 26.5], "title": "playing kittens", "content": "There are several kittens playing around the puppy", "meta": "bar"},
    ],
)

# passing `use_tantivy=False` to use lance FTS index
# `use_tantivy=True` by default
table.create_fts_index("content", use_tantivy=True)
table.search("puppy").limit(10).select(["content"]).to_list()
# [{'text': 'Frodo was a happy puppy', '_score': 0.6931471824645996}]
# ...
```

It would search on all indexed columns by default, so it's useful when there are multiple indexed columns.

!!! note
    LanceDB automatically searches on the existing FTS index if the input to the search is of type `str`. If you provide a vector as input, LanceDB will search the ANN index instead.

## Tokenization
By default the text is tokenized by splitting on punctuation and whitespaces and then removing tokens that are longer than 40 chars. For more language specific tokenization then provide the argument tokenizer_name with the 2 letter language code followed by "_stem". So for english it would be "en_stem".

```python
table.create_fts_index("content", use_tantivy=True, tokenizer_name="en_stem", replace=True)
```

the following [languages](https://docs.rs/tantivy/latest/tantivy/tokenizer/enum.Language.html) are currently supported.

## Index multiple columns

If you have multiple string columns to index, there's no need to combine them manually -- simply pass them all as a list to `create_fts_index`:

```python
table.create_fts_index(["title", "content"], use_tantivy=True, replace=True)
```

Note that the search API call does not change - you can search over all indexed columns at once.

## Filtering

Currently the LanceDB full text search feature supports *post-filtering*, meaning filters are
applied on top of the full text search results (see [native FTS](fts.md) if you need pre-filtering). This can be invoked via the familiar
`where` syntax:

```python
table.search("puppy").limit(10).where("meta='foo'").to_list()
```

## Sorting

You can pre-sort the documents by specifying `ordering_field_names` when
creating the full-text search index. Once pre-sorted, you can then specify
`ordering_field_name` while searching to return results sorted by the given
field. For example,

```python
table.create_fts_index(["content"], use_tantivy=True, ordering_field_names=["id"], replace=True)

(table.search("puppy", ordering_field_name="id")
 .limit(20)
 .to_list())
```

!!! note
    If you wish to specify an ordering field at query time, you must also
    have specified it during indexing time. Otherwise at query time, an
    error will be raised that looks like `ValueError: The field does not exist: xxx`

!!! note
    The fields to sort on must be of typed unsigned integer, or else you will see
    an error during indexing that looks like
    `TypeError: argument 'value': 'float' object cannot be interpreted as an integer`.

!!! note
    You can specify multiple fields for ordering at indexing time.
    But at query time only one ordering field is supported.


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
1. Explicitly declare the `phrase_query()` method. This is useful when you have a phrase query that
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
table.create_fts_index(["title", "content"], use_tantivy=True, writer_heap_size=heap, replace=True)
```

## Current limitations

1. New data added after creating the FTS index will appear in search results, but with increased latency due to a flat search on the unindexed portion. Re-indexing with `create_fts_index` will reduce latency. LanceDB Cloud automates this merging process, minimizing the impact on search speed. 

2. We currently only support local filesystem paths for the FTS index.
   This is a tantivy limitation. We've implemented an object store plugin
   but there's no way in tantivy-py to specify to use it.
