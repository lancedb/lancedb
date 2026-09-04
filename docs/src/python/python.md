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

## Remote SQL

Submit SQL against a remote LanceDB database through the connection.
The connected database and `default_namespace_path=["public"]` are used for
unqualified tables. Fully qualified references can still query other databases
and namespaces available to the same deployment. `execute_query` returns a
reader as soon as its initial result stream is available. `execute_query_async`
returns a query handle immediately; use it to inspect progress, open a reader,
or cancel the query. The SQL client is initialized by the first query and
retained for the lifetime of the remote connection. Query ids are random,
connection-scoped references rather than encoded SQL or durable resume tokens:

```python
import lancedb

db = lancedb.connect(
    "db://analytics",
    api_key="ldb_...",
    host_override="https://api.example.com",
    sql_host_override="grpc+tls://sql.example.com:10026",
)
reader = db.execute_query(
    """
    SELECT events.id, accounts.name
    FROM analytics.public.events AS events
    JOIN users.public.accounts AS accounts ON events.user_id = accounts.id
    """,
    default_namespace_path=["public"],
)
for batch in reader:
    print(batch.num_rows)

query = db.execute_query_async("SELECT * FROM events")
print(query.id)
print(query.describe().status)
for batch in query.reader():
    print(batch.num_rows)

# The async connection exposes the same lifecycle without blocking:
# async_db = await lancedb.connect_async(
#     "db://analytics",
#     api_key="ldb_...",
#     host_override="https://api.example.com",
#     sql_host_override="grpc+tls://sql.example.com:10026",
# )
# reader = await async_db.execute_query("SELECT * FROM events")
# query = await async_db.execute_query_async("SELECT * FROM events")
# description = await async_db.describe_query(query.id)
# async for batch in await query.reader():
#     print(batch.num_rows)
# await query.cancel()
```

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

## Functions and Jobs

::: lancedb.functions.FunctionArtifact

::: lancedb.functions.FunctionParameter

::: lancedb.functions.FunctionResultField

::: lancedb.functions.FunctionOutput

::: lancedb.functions.FunctionSignature

::: lancedb.functions.PythonEnvironmentSpec

::: lancedb.functions.udf

::: lancedb.functions.UdfDefinition

::: lancedb.functions.FunctionRegistrationRequest

::: lancedb.functions.FunctionArtifactRequest

::: lancedb.functions.FunctionArtifactContent

::: lancedb.functions.PythonAdapterSpec

::: lancedb.functions.FunctionVersion

::: lancedb.functions.PythonRuntimeSpec

::: lancedb.functions.FunctionVersionRef

::: lancedb.functions.ApplicationInput

::: lancedb.functions.FunctionApplication

::: lancedb.functions.InputBinding

::: lancedb.functions.OutputMapping

::: lancedb.functions.AssignmentMapping

::: lancedb.functions.FunctionBinding

::: lancedb.functions.RefreshColumnResult

::: lancedb.job.Job

::: lancedb.job.AsyncJob

::: lancedb.sql.Query

::: lancedb.sql.AsyncQuery

::: lancedb.sql.QueryDescription

## Materialized Views (Synchronous)

::: lancedb.materialized_view.MaterializedView

::: lancedb.materialized_view.MaterializedViewDefinition

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

::: lancedb.query.DocumentGranularity

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

tokens = list(
    lancedb.tokenize("acme makes searchable data", custom_stop_words=["acme"])
)
```

::: lancedb.tokenize

::: lancedb.FtsToken

## Blobs

Blob columns store large binary values out of line so they can be read lazily
instead of being materialized with the rest of the row.

`lancedb.BlobType` is `lance.blob.BlobType` when pylance is installed. Without
pylance, LanceDB uses a matching `lance.blob.v2` extension type so blob columns
still work. Queries return descriptors. Call
[`fetch_blob_files`][lancedb.table.Table.fetch_blob_files] for lazy reads or
[`fetch_blobs`][lancedb.table.Table.fetch_blobs] for eager bytes.

::: lancedb.blob

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

::: lancedb.streaming.StreamingDataLoader

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

## Materialized Views (Asynchronous)

::: lancedb.materialized_view.AsyncMaterializedView

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

## Runtime lifecycle (advanced)

Using LanceDB creates a Tokio runtime, a background event loop thread and an
embedding thread pool that are held for the entire life of the process.
Outside of `fork()` — which does not exist on Windows — nothing recycles
them. Long-lived hosts that use LanceDB in bursts (test suites, notebook
kernels, agent runtimes, services that re-index periodically) can release
them explicitly. Not needed for typical usage.

::: lancedb.background_loop.reset_background_loop
