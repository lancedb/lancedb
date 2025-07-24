# Full-text search (Native FTS)

LanceDB provides support for full-text search via Lance, allowing you to incorporate keyword-based search (based on BM25) in your retrieval solutions.

!!! note
    The Python SDK uses tantivy-based FTS by default, need to pass `use_tantivy=False` to use native FTS.

## Example

Consider that we have a LanceDB table named `my_table`, whose string column `text` we want to index and query via keyword search, the FTS index must be created before you can search via keywords.

=== "Python"
    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb-fts"
        --8<-- "python/python/tests/docs/test_search.py:basic_fts"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb"
        --8<-- "python/python/tests/docs/test_search.py:import-lancedb-fts"
        --8<-- "python/python/tests/docs/test_search.py:basic_fts_async"
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
        .search("puppy", "fts")
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

The search will include all indexed columns by default, which is useful when there are multiple indexed columns.

Passing `fts_columns="text"` if you want to specify the columns to search.

!!! note
    LanceDB automatically searches on the existing FTS index if the input to the search is of type `str`. If you provide a vector as input, LanceDB will search the ANN index instead.

## Tokenization
By default the text is tokenized by splitting on punctuation and whitespaces, and would filter out words that are with length greater than 40, and lowercase all words.

Stemming is useful for improving search results by reducing words to their root form, e.g. "running" to "run". LanceDB supports stemming for multiple languages, you can specify the tokenizer name to enable stemming by the pattern `tokenizer_name="{language_code}_stem"`, e.g. `en_stem` for English.

For example, to enable stemming for English:
=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_config_stem"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_config_stem_async"
    ```

the following [languages](https://docs.rs/tantivy/latest/tantivy/tokenizer/enum.Language.html) are currently supported.

The tokenizer is customizable, you can specify how the tokenizer splits the text, and how it filters out words, etc.

For example, for language with accents, you can specify the tokenizer to use `ascii_folding` to remove accents, e.g. 'é' to 'e':
=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_config_folding"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_config_folding_async"
    ```

## Filtering

LanceDB full text search supports to filter the search results by a condition, both pre-filtering and post-filtering are supported.

This can be invoked via the familiar `where` syntax.

With pre-filtering:
=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_prefiltering"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_prefiltering_async"
        ```

=== "TypeScript"

    ```typescript
    await tbl
    .search("puppy")
    .select(["id", "doc"])
    .limit(10)
    .where("meta='foo'")
    .prefilter(true)
    .toArray();
    ```

=== "Rust"

    ```rust
    table
        .query()
        .full_text_search(FullTextSearchQuery::new("puppy".to_owned()))
        .select(lancedb::query::Select::Columns(vec!["doc".to_owned()]))
        .limit(10)
        .only_if("meta='foo'")
        .execute()
        .await?;
    ```

With post-filtering:
=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_postfiltering"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_postfiltering_async"
        ```

=== "TypeScript"

    ```typescript
    await tbl
    .search("apple")
    .select(["id", "doc"])
    .limit(10)
    .where("meta='foo'")
    .prefilter(false)
    .toArray();
    ```

=== "Rust"

    ```rust
    table
        .query()
        .full_text_search(FullTextSearchQuery::new(words[0].to_owned()))
        .select(lancedb::query::Select::Columns(vec!["doc".to_owned()]))
        .postfilter()
        .limit(10)
        .only_if("meta='foo'")
        .execute()
        .await?;
    ```

## Phrase queries vs. terms queries

!!! warning "Warn"
    Lance-based FTS doesn't support queries using boolean operators `OR`, `AND`.

For full-text search you can specify either a **phrase** query like `"the old man and the sea"`,
or a **terms** search query like `old man sea`. For more details on the terms
query syntax, see Tantivy's [query parser rules](https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html).

To search for a phrase, the index must be created with `with_position=True`:
=== "Sync API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_with_position"
    ```
=== "Async API"

    ```python
    --8<-- "python/python/tests/docs/test_search.py:fts_with_position_async"
    ```
This will allow you to search for phrases, but it will also significantly increase the index size and indexing time.


## Incremental indexing

LanceDB supports incremental indexing, which means you can add new records to the table without reindexing the entire table.

This can make the query more efficient, especially when the table is large and the new records are relatively small.

=== "Python"

    === "Sync API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_incremental_index"
        ```
    === "Async API"

        ```python
        --8<-- "python/python/tests/docs/test_search.py:fts_incremental_index_async"
        ```

=== "TypeScript"

    ```typescript
    await tbl.add([{ vector: [3.1, 4.1], text: "Frodo was a happy puppy" }]);
    await tbl.optimize();
    ```

=== "Rust"

    ```rust
    let more_data: Box<dyn RecordBatchReader + Send> = create_some_records()?;
    tbl.add(more_data).execute().await?;
    tbl.optimize(OptimizeAction::All).execute().await?;
    ```
!!! note

    New data added after creating the FTS index will appear in search results while incremental indexing is still in progress, but with increased latency due to a flat search on the unindexed portion. LanceDB Cloud automates this merging process, minimizing the impact on search speed.
