# Full-text search

LanceDB provides support for full-text search via Lance (before via [Tantivy](https://github.com/quickwit-oss/tantivy) (Python only)), allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions.

Currently, the Lance full text search is missing some features that are in the Tantivy full text search. This includes phrase queries, re-ranking, and customizing the tokenizer. Thus, in Python, Tantivy is still the default way to do full text search and many of the instructions below apply just to Tantivy-based indices.


## Installation (Only for Tantivy-based FTS)

!!! note
    No need to install the tantivy dependency if using native FTS

To use full-text search, install the dependency [`tantivy-py`](https://github.com/quickwit-oss/tantivy-py):

```sh
# Say you want to use tantivy==0.20.1
pip install tantivy==0.20.1
```

## Example

Consider that we have a LanceDB table named `my_table`, whose string column `text` we want to index and query via keyword search, the FTS index must be created before you can search via keywords.

=== "Python"

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

    # passing `use_tantivy=False` to use lance FTS index
    # `use_tantivy=True` by default
    table.create_fts_index("text")
    table.search("puppy").limit(10).select(["text"]).to_list()
    # [{'text': 'Frodo was a happy puppy', '_score': 0.6931471824645996}]
    # ...
    ```

=== "TypeScript"

    ```typescript
    import * as lancedb from "@lancedb/lancedb";
    const uri = "data/sample-lancedb"
    const db = await lancedb.connect(uri);

    const data = [
    { vector: [3.1, 4.1], text: "Frodo was a happy puppy" },
    { vector: [5.9, 26.5], text: "There are several kittens playing" },
    ];
    const tbl = await db.createTable("my_table", data, { mode: "overwrite" });
    await tbl.createIndex("text", {
        config: lancedb.Index.fts(),
    });

    await tbl
        .search("puppy")
        .select(["text"])
        .limit(10)
        .toArray();
    ```

=== "Rust"

    ```rust
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    let initial_data: Box<dyn RecordBatchReader + Send> = create_some_records()?;
    let tbl = db
        .create_table("my_table", initial_data)
        .execute()
        .await?;
    tbl
        .create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await?;

    tbl
        .query()
        .full_text_search(FullTextSearchQuery::new("puppy".to_owned()))
        .select(lancedb::query::Select::Columns(vec!["text".to_owned()]))
        .limit(10)
        .execute()
        .await?;
    ```

It would search on all indexed columns by default, so it's useful when there are multiple indexed columns.
For now, this is supported in tantivy way only.

Passing `fts_columns="text"` if you want to specify the columns to search, but it's not available for Tantivy-based full text search.

!!! note
    LanceDB automatically searches on the existing FTS index if the input to the search is of type `str`. If you provide a vector as input, LanceDB will search the ANN index instead.

## Tokenization
By default the text is tokenized by splitting on punctuation and whitespaces and then removing tokens that are longer than 40 chars. For more language specific tokenization then provide the argument tokenizer_name with the 2 letter language code followed by "_stem". So for english it would be "en_stem".

For now, only the Tantivy-based FTS index supports to specify the tokenizer, so it's only available in Python with `use_tantivy=True`.

=== "use_tantivy=True"

    ```python
    table.create_fts_index("text", use_tantivy=True, tokenizer_name="en_stem")
    ```

=== "use_tantivy=False"

    [**Not supported yet**](https://github.com/lancedb/lance/issues/1195)

the following [languages](https://docs.rs/tantivy/latest/tantivy/tokenizer/enum.Language.html) are currently supported.

## Index multiple columns

If you have multiple string columns to index, there's no need to combine them manually -- simply pass them all as a list to `create_fts_index`:

=== "use_tantivy=True"

    ```python
    table.create_fts_index(["text1", "text2"])
    ```

=== "use_tantivy=False"

    [**Not supported yet**](https://github.com/lancedb/lance/issues/1195)

Note that the search API call does not change - you can search over all indexed columns at once.

## Filtering

Currently the LanceDB full text search feature supports *post-filtering*, meaning filters are
applied on top of the full text search results. This can be invoked via the familiar
`where` syntax:

=== "Python"

    ```python
    table.search("puppy").limit(10).where("meta='foo'").to_list()
    ```

=== "TypeScript"

    ```typescript
    await tbl
    .search("apple")
    .select(["id", "doc"])
    .limit(10)
    .where("meta='foo'")
    .toArray();
    ```

=== "Rust"

    ```rust
    table
        .query()
        .full_text_search(FullTextSearchQuery::new(words[0].to_owned()))
        .select(lancedb::query::Select::Columns(vec!["doc".to_owned()]))
        .limit(10)
        .only_if("meta='foo'")
        .execute()
        .await?;
    ```

## Sorting

!!! warning "Warn"
    Sorting is available for only Tantivy-based FTS

You can pre-sort the documents by specifying `ordering_field_names` when
creating the full-text search index. Once pre-sorted, you can then specify
`ordering_field_name` while searching to return results sorted by the given
field. For example,

```python
table.create_fts_index(["text_field"], use_tantivy=True, ordering_field_names=["sort_by_field"])

(table.search("terms", ordering_field_name="sort_by_field")
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

!!! warning "Warn"
    Phrase queries are available for only Tantivy-based FTS

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


## Configurations (Only for Tantivy-based FTS)

By default, LanceDB configures a 1GB heap size limit for creating the index. You can
reduce this if running on a smaller node, or increase this for faster performance while
indexing a larger corpus.

```python
# configure a 512MB heap size
heap = 1024 * 1024 * 512
table.create_fts_index(["text1", "text2"], writer_heap_size=heap, replace=True)
```

## Current limitations

For that Tantivy-based FTS:

1. Currently we do not yet support incremental writes.
   If you add data after FTS index creation, it won't be reflected
   in search results until you do a full reindex.

2. We currently only support local filesystem paths for the FTS index.
   This is a tantivy limitation. We've implemented an object store plugin
   but there's no way in tantivy-py to specify to use it.
