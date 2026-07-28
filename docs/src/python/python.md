# Python API Reference

This section contains the API reference for the Python API of [LanceDB](https://github.com/lancedb/lancedb). Both synchronous and asynchronous APIs are available.

The general flow of using the API is:

1. Use [lancedb.connect][] or [lancedb.connect_async][] to connect to a database.
2. Use the returned [lancedb.DBConnection][] or [lancedb.AsyncConnection][] to
   create or open tables.
3. Use the returned [lancedb.table.Table][] or [lancedb.AsyncTable][] to query
   or modify tables.


## Installation

```shell
pip install lancedb
```

The following methods describe the synchronous API client. There
is also an [asynchronous API client](#connections-asynchronous).

## Connections (Synchronous)

::: lancedb.connect

::: lancedb.db.DBConnection

## Tables (Synchronous)

::: lancedb.table.Table

::: lancedb.table.FragmentStatistics

::: lancedb.table.FragmentSummaryStats

::: lancedb.table.Tags

## Expressions

Type-safe expression builder for filters and projections. Use these instead
of raw SQL strings with [where][lancedb.query.LanceQueryBuilder.where] and
[select][lancedb.query.LanceQueryBuilder.select].

::: lancedb.expr.Expr

::: lancedb.expr.col

::: lancedb.expr.lit

::: lancedb.expr.func

## Querying (Synchronous)

::: lancedb.query.Query

::: lancedb.query.LanceQueryBuilder

::: lancedb.query.LanceVectorQueryBuilder

::: lancedb.query.LanceFtsQueryBuilder

::: lancedb.query.LanceHybridQueryBuilder

## Embeddings

::: lancedb.embeddings.registry.EmbeddingFunctionRegistry

::: lancedb.embeddings.base.EmbeddingFunctionConfig

::: lancedb.embeddings.base.EmbeddingFunction

::: lancedb.embeddings.base.TextEmbeddingFunction

::: lancedb.embeddings.sentence_transformers.SentenceTransformerEmbeddings

::: lancedb.embeddings.openai.OpenAIEmbeddings

::: lancedb.embeddings.open_clip.OpenClipEmbeddings

## Remote configuration

::: lancedb.remote.ClientConfig

::: lancedb.remote.TimeoutConfig

::: lancedb.remote.RetryConfig

## Context

::: lancedb.context.contextualize

::: lancedb.context.Contextualizer

## Full text search

Use [lancedb.table.Table.create_index][] or
[lancedb.table.AsyncTable.create_index][] with [lancedb.index.FTS][]:

```python
from lancedb.index import FTS

table.create_index(
    "text",
    config=FTS(
        remove_stop_words=True,
        custom_stop_words=["acme", "internal"],
    ),
)
```

The asynchronous form uses the same configuration:

```python
await async_table.create_index("text", config=FTS(custom_stop_words=["acme"]))
```

### Custom stop-word sources

`custom_stop_words` accepts exactly one source: an inline list, a UTF-8
newline-delimited file, or a string column from an open local LanceDB table.

```python
from lancedb.index import FTS, FtsStopWordsFile, FtsStopWordsTable

# Inline snapshot
inline = FTS(custom_stop_words=["acme", "internal"])

# One stop word per line in a strict UTF-8 file
from_file = FTS(custom_stop_words=FtsStopWordsFile("stop_words.txt"))

# Values from the "word" string column of an already opened local table
stop_words_table = db.open_table("stop_words")
from_table = FTS(
    custom_stop_words=FtsStopWordsTable(stop_words_table, "word")
)

table.create_index("text", config=from_table)
```

The source is resolved when `create_index` runs. LanceDB stores the resulting
word-list snapshot with the index, so reopening, rebuilding, querying, and
index-backed tokenization use the same words. A remote target index may use a
client-local file or local LanceDB table source: the client resolves it first
and sends only the concrete string list, so the service never attempts to open
a client-local path or interpret a table reference. A remote table cannot
itself be a source because the client cannot guarantee an untruncated snapshot;
materialize that column into a local table or pass an inline list instead.
Changing the original file or source table does not change an existing index;
recreate the index to take a new snapshot.

The stop-word semantics are:

- `custom_stop_words=None` uses the built-in list for `language`.
- `custom_stop_words=[]` explicitly replaces the built-in list with an empty
  list, so no stop words are removed.
- A non-empty custom list replaces the built-in language list; it is not
  appended to it.
- The configured snapshot is only applied when `remove_stop_words=True`.
- LanceDB does not trim, lowercase, or otherwise rewrite stop words. Exact
  duplicates are removed while preserving the first occurrence. Exact empty
  entries, including empty file lines, are ignored.
- File decoding is strict UTF-8. Table sources require the named column to
  contain only non-null strings. Missing or unreadable files, invalid UTF-8,
  empty paths or column names, unavailable tables, remote table sources,
  missing columns, non-string columns, and null values raise an error instead
  of silently dropping words.

Local FTS queries with active custom stop words reject an explicit
`fuzziness > 0`: upstream fuzzy matching does not reuse the index's complete
tokenizer configuration, so LanceDB fails closed instead of silently applying
different stop-word semantics. An omitted fuzziness value and `fuzziness=0`
continue to use the configured snapshot. Remote tables temporarily reject every
explicitly positive `fuzziness` before sending a query request, even without
custom stop words, because the service does not yet expose a capability that
atomically binds the query to its tokenizer snapshot; this also avoids a
list-details/query time-of-check/time-of-use race. Native namespace pushdown
routes explicitly fuzzy queries to local execution, where the local rule
applies.

The standalone tokenizer accepts the same source forms:

```python
import lancedb
from lancedb.index import FtsStopWordsFile

tokens = list(
    lancedb.tokenize(
        "acme makes searchable data",
        custom_stop_words=FtsStopWordsFile("stop_words.txt"),
    )
)
```

`table.tokenize(...)` and `async_table.tokenize(...)` instead load the
persisted snapshot from the selected FTS index. The deprecated
[lancedb.table.Table.create_fts_index][] also accepts `custom_stop_words` for
compatibility, but new code should use `create_index(..., config=FTS(...))`.
When that deprecated helper also receives `tokenizer_name`, it preserves the
alias's historical `remove_stop_words=False`: the snapshot is persisted but
inactive. Use the modern `FTS(remove_stop_words=True, ...)` form to enable it.

::: lancedb.index.FTS

::: lancedb.index.FtsStopWordsFile

::: lancedb.index.FtsStopWordsTable

## Utilities

::: lancedb.schema.vector

::: lancedb.merge.LanceMergeInsertBuilder

## Integrations

## Pydantic

::: lancedb.pydantic.pydantic_to_schema

::: lancedb.pydantic.vector

::: lancedb.pydantic.LanceModel

## Reranking

::: lancedb.rerankers.linear_combination.LinearCombinationReranker

::: lancedb.rerankers.cohere.CohereReranker

::: lancedb.rerankers.colbert.ColbertReranker

::: lancedb.rerankers.cross_encoder.CrossEncoderReranker

::: lancedb.rerankers.openai.OpenaiReranker

## Connections (Asynchronous)

Connections represent a connection to a LanceDb database and
can be used to create, list, or open tables.

::: lancedb.connect_async

::: lancedb.db.AsyncConnection

## Tables (Asynchronous)

Table hold your actual data as a collection of records / rows.

::: lancedb.table.AsyncTable

::: lancedb.table.AsyncTags

## Indices (Asynchronous)

Indices can be created on a table to speed up queries. This section
lists the indices that LanceDb supports.

::: lancedb.index.BTree

::: lancedb.index.Bitmap

::: lancedb.index.LabelList

::: lancedb.index.FTS

::: lancedb.index.IvfPq

::: lancedb.index.HnswPq

::: lancedb.index.HnswSq

::: lancedb.index.IvfFlat

::: lancedb.index.IvfSq

::: lancedb.index.IvfRq

::: lancedb.index.HnswFlat

::: lancedb.table.IndexStatistics

## Querying (Asynchronous)

Queries allow you to return data from your database. Basic queries can be
created with the [AsyncTable.query][lancedb.table.AsyncTable.query] method
to return the entire (typically filtered) table. Vector searches return the
rows nearest to a query vector and can be created with the
[AsyncTable.vector_search][lancedb.table.AsyncTable.vector_search] method.


::: lancedb.query.AsyncQuery
    options:
      inherited_members: true

::: lancedb.query.AsyncVectorQuery
    options:
      inherited_members: true

::: lancedb.query.AsyncFTSQuery
    options:
      inherited_members: true

::: lancedb.query.AsyncHybridQuery
    options:
      inherited_members: true
