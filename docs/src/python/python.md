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

::: lancedb.Session

## Namespaces (Synchronous)

A namespace-backed connection resolves tables through a
[Lance namespace](https://lance-format.github.io/lance-namespace/) service instead of
listing a storage directory.

::: lancedb.connect_namespace

::: lancedb.namespace.LanceNamespaceDBConnection

## Tables (Synchronous)

::: lancedb.table.Table

::: lancedb.table.FragmentStatistics

::: lancedb.table.FragmentSummaryStats

::: lancedb.table.TableStatistics

::: lancedb.table.Tags

::: lancedb.table.Branches

::: lancedb.LsmWriteSpec

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

::: lancedb.query.LanceEmptyQueryBuilder

::: lancedb.query.LanceTakeQueryBuilder

## Full text queries

Structured full text queries can be passed to
[Table.search][lancedb.table.Table.search] or
[AsyncTable.search][lancedb.table.AsyncTable.search] in place of a query string,
and combined with [BooleanQuery][lancedb.query.BooleanQuery].

::: lancedb.query.FullTextQuery

::: lancedb.query.MatchQuery

::: lancedb.query.PhraseQuery

::: lancedb.query.BoostQuery

::: lancedb.query.MultiMatchQuery

::: lancedb.query.BooleanQuery

::: lancedb.query.FullTextOperator

::: lancedb.query.Occur

## Embeddings

::: lancedb.embeddings
    options:
      show_root_heading: false
      show_root_toc_entry: false

## Remote configuration

::: lancedb.remote
    options:
      show_root_heading: false
      show_root_toc_entry: false

## Context

::: lancedb.context.contextualize

::: lancedb.context.Contextualizer

## Full text search

Pass `custom_stop_words` to [lancedb.index.FTS][]:

```python
from lancedb.index import FTS

table.create_index(
    "text",
    config=FTS(remove_stop_words=True, custom_stop_words=["acme", "internal"]),
)
```

The list replaces the built-in stop words and is used only when
`remove_stop_words=True`:

- `custom_stop_words=None` uses the built-in list for `language`.
- `custom_stop_words=[]` removes no words.
- Values are passed through without trimming, lowercasing, or other rewriting.

The same option is available on `lancedb.tokenize(...)` and the deprecated
[lancedb.table.Table.create_fts_index][] compatibility helper:

```python
import lancedb

tokens = list(lancedb.tokenize("acme makes searchable data",
                               custom_stop_words=["acme"]))
```

::: lancedb.tokenize

::: lancedb.FtsToken

## Blobs

Blob columns store large binary values out of line so they can be read lazily
instead of being materialized with the rest of the row.

::: lancedb.blob

::: lancedb.BlobType

::: lancedb._blob.BlobFile
    options:
      show_root_full_path: false

## Utilities

::: lancedb.schema.vector

::: lancedb.merge.LanceMergeInsertBuilder

::: lancedb.otel.instrument_lancedb_metrics

## Exceptions

::: lancedb.exceptions.MissingValueError

::: lancedb.exceptions.MissingColumnError

## Integrations

## Pydantic

::: lancedb.pydantic.pydantic_to_schema

::: lancedb.pydantic.vector

::: lancedb.pydantic.Vector

::: lancedb.pydantic.MultiVector

::: lancedb.pydantic.LanceModel

## PyTorch

::: lancedb.streaming.StreamingDataset

::: lancedb.permutation.permutation_builder

::: lancedb.permutation.PermutationBuilder

::: lancedb.permutation.Permutation

::: lancedb.permutation.Transforms

## Reranking

::: lancedb.rerankers
    options:
      show_root_heading: false
      show_root_toc_entry: false

## Connections (Asynchronous)

Connections represent a connection to a LanceDb database and
can be used to create, list, or open tables.

::: lancedb.connect_async

::: lancedb.db.AsyncConnection

## Namespaces (Asynchronous)

::: lancedb.connect_namespace_async

::: lancedb.namespace.AsyncLanceNamespaceDBConnection

## Tables (Asynchronous)

Table hold your actual data as a collection of records / rows.

::: lancedb.table.AsyncTable

::: lancedb.table.AsyncTags

::: lancedb.table.AsyncBranches

## Indices (Asynchronous)

Indices can be created on a table to speed up queries. This section
lists the indices that LanceDb supports.

::: lancedb.index
    options:
      show_root_heading: false
      show_root_toc_entry: false
      # `lang_mapping` is defined in the module rather than imported, so it is
      # picked up despite not being in `__all__`. It is an internal lookup table.
      filters: ["!^_", "!^lang_mapping$"]

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

::: lancedb.query.AsyncTakeQuery
    options:
      inherited_members: true
