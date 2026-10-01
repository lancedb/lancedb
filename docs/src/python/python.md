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

Resource listings (`list_secrets`, `list_views`, `list_jobs`, `list_functions`, and
`list_materialized_views`) return lazy iterators. Use `list(db.list_views())` to
collect synchronous results, or `[name async for name in db.list_views()]` with
an asynchronous connection. `page_limit` controls each request, and `page_token`
resumes a listing. Drain the iterator's cached results before saving its token.

::: lancedb.listing.Listing

::: lancedb.Session

## Catalogs (Synchronous)

Remote catalogs manage databases through a server's root namespace. Opened databases
are ordinary connections. Dropping a database requires it to be empty.

::: lancedb.connect_catalog

::: lancedb.catalog.Catalog

::: lancedb.catalog.DatabaseNames


## Authorization

Some catalogs support the authorization API. This is an API that can be used to
bind access control lists (ACLs) to individual catalog objects.

Each ACL contains a subject, an object, and privilege.

### Subjects

There are three kinds of subjects: principals, groups, and roles.

A principal uniquely identifies each user accessing the system. Each LanceDB
Enterprise API key corresponds to a unique principal, and each OpenID user has
a unique principal.

All principals have an immutable ID which never changes, and is safe to include
in log files. For example, the API key
`sk_AAAQEAYEAUDAOCAJBIFQYDIOB4IBCEQTCQKRMFYYDENBWHA5DYPQ====` maps to the
principal ID `pk_aaaqeayeaudaocajbifqydiob4`. APIs such as `list_acls` will
return the principal ID, never the API key.

OpenID principals also typically have a human-readable name associated with
them. For example, a principal might be named `bob@example.com` and might have
a principal ID of `8a004400-9443-4fab-9997-b9372a7ca8fdCopy`. Whenever
possible, APIs will return the principal name for convenience. However,
principal names are mutable and can be changed by the identity provider.

Here is an example of getting information about an API key:
```python
from lancedb.authz import Subject
catalog = lancedb.connect_catalog("https://catalog.example.com", api_key=api_key)
principal = catalog.authz.get_principal(Subject.principal_api_key(api_key))
print(f"Found principal {principal.principal_id}")
```

OpenID principals can be members of groups. Like principals, groups also have
IDs and human-readable names. It is not possible to modify group membership via
this API; it is managed by the OpenID identity provider.

Here is an example of getting information about a group based on its name:
```python
from lancedb.authz import Subject
catalog = lancedb.connect_catalog("https://catalog.example.com", api_key=api_key)
group = catalog.authz.get_group(Subject.group_name("Engineering"))
print(f"Found group {group.group_id} with name {group.group_name}")
```

Roles can contain either principals or groups. Unlike groups, they are managed
by the catalog, not the identity provider. Roles can also contain API key
principals.

Here is an example of managing the `reader` role:
```python
from lancedb.authz import Subject
catalog = lancedb.connect_catalog("https://catalog.example.com", api_key=api_key)
catalog.authz.add_role_binding(
    subject=Subject.principal_name("Alice"),
    role="reader"
)
```
It may take a few milliseconds for changes to role bindings to become visible
to all clients.

More subject types may be added in the future.

### Objects

An object is any resource in the system that we can attach authorization rules to.
The Python builders and Rust client share resource validation and wire encoding.
Methods also accept literal strings such as `table:tenant/db:public$events`;
malformed names for known resource types raise `ValueError` locally before a
request is sent. Unknown resource types are preserved for compatibility with
newer servers.

Object types include:

The System object, a singleton which represents the entire system. You can
refer to it as `Object.system()`.

Database objects which represents specific databases. For example,
`Object.database("analytics")` represents the "analytics" database.

Table objects which represent specific tables. For example,
`Object.table(database="analytics", name="events")` represents the "events"
table inside the "analytics" database. Tables are in the default namespace (which
is named "public") unless a specific namespaces is given.

Similarly, View objects represent specific views. For example,
`Object.view(database="analytics", name="recent_events")` represents the
"recent_events" view inside the "analytics" table. Views are in the default
namespace (which is named "public") unless a specific namespaces is given.

Secret objects identify named credentials, for example,
`Object.secret(database="analytics", name="api-key")`. Function objects identify
registered functions, for example,
`Object.function(database="analytics", name="caption")`. Both default to the
`public` namespace. Pass `namespace=["platform", "ml"]` to select a nested
namespace. These objects identify resources for ACL operations; a secret object
contains the credential's name, not its value.

More object types may be added in the future.

### Access control lists

An access control list attaches a specific set of privileges to a specific
(subject, object) pair. So, for example, we might have an access control list
that gives `bob@example.com` the ability to read the foo table, that looks like
this:

```python
from lancedb.authz import Object, Subject, Privilege

catalog = lancedb.connect_catalog("https://catalog.example.com", api_key=api_key)
catalog.authz.add_acl(
    object=Object.table(database="analytics", name="foo"),
    subject=Subject.principal_name("bob@example.com"),
    privilege=Privilege.SELECT,
)
```

Reading a table generally requires USAGE on its database and namespace, plus
SELECT on the table. Each object has at most one owner.

OWNERSHIP is a special privilege which grants all the other privileges. By
default, the principal who created an object owns it. Only one subject can own
an object; granting OWNERSHIP to a new subject removes it from the previous
owner. OWNERSHIP cannot be deleted.

To delegate secret or function creation, grant `CREATE_SECRET` or
`CREATE_FUNCTION` on the containing namespace. This preserves namespace ownership:

```python
for privilege in [Privilege.CREATE_SECRET, Privilege.CREATE_FUNCTION]:
    catalog.authz.add_acl(
        object=Object.namespace(database="analytics", namespace=["public"]),
        subject=Subject.role("developer"),
        privilege=privilege,
    )
```

Both privileges also accept their string names in synchronous and asynchronous
ACL methods.

By default, the `list_acls` function lists all the access control entries in
the system. By adding a function parameter, the output is filtered to just ACLs
matching that parameter. For example, this code lists all the ACLs for the
"reader" role:

```python
from lancedb.authz import Subject
catalog = lancedb.connect_catalog("https://catalog.example.com", api_key=api_key)
for entry in catalog.authz.list_acls(subject=Subject.role("reader")):
    print(entry)
```

Or, when using the async API:
```python
catalog = await lancedb.connect_catalog_async(endpoint, api_key=api_key)
async for entry in catalog.authz.list_acls(subject=Subject.role("reader")):
    print(entry)
```

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

# Values bind to $1 / $name placeholders and travel as Arrow, so a float32
# stays a float32 and a vector stays a compact fixed-size list:
import numpy as np

reader = db.execute_query(
    "SELECT id FROM docs ORDER BY distance(vector, $vector) LIMIT $k",
    parameters={"vector": np.random.rand(768).astype(np.float32), "k": 10},
)

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

::: lancedb.secrets.EnvVarSecret

::: lancedb.secrets.SecretInfo

::: lancedb.functions.FunctionRegistrationRequest

::: lancedb.functions.FunctionArtifactRequest

::: lancedb.functions.FunctionArtifactContent

::: lancedb.functions.PythonAdapterSpec

::: lancedb.functions.FunctionImage

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

::: lancedb.FunctionErrors

::: lancedb.FunctionErrorRecord

::: lancedb.FunctionErrorFragment

::: lancedb.job.Job

::: lancedb.job.AsyncJob

::: lancedb.job.JobInfo

::: lancedb.job.JobDescription

::: lancedb.job.JobFailureInfo

::: lancedb.sql.Query

::: lancedb.sql.AsyncQuery

::: lancedb.sql.QueryDescription

::: lancedb.sql.QueryParameters

## Materialized Views (Synchronous)

::: lancedb.materialized_view.MaterializedView

::: lancedb.materialized_view.MaterializedViewDefinition

## Views

::: lancedb.view.ViewDescription

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

::: lancedb.remote.errors.HttpError

::: lancedb.exceptions.MissingValueError

::: lancedb.exceptions.MissingColumnError

::: lancedb.exceptions.JobNotFoundError

::: lancedb.exceptions.JobFailedError

::: lancedb.exceptions.JobCancelledError

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

`TypeSafeReranker` supports opt-in request batching:

```python
from lancedb.rerankers import TypeSafeReranker

reranker = TypeSafeReranker(batch_size=40, max_concurrency=8)
```

The default `batch_size=1` keeps the query and document in request state and
sends one request per non-null candidate. With `batch_size=40`, 80 non-null
candidates require two requests. `max_concurrency` still limits simultaneous
requests, and the SDK handles retries.

In batched mode, state contains only `{"query": query}`. Each independent
question contains `{"question": instructions, "document": document}` in its
structured instructions, so it sees only its own document and the shared query.
Custom instructions and criteria are kept verbatim: adapt prompts that explicitly
reference request-state fields such as `state.document` before enabling batching.
Null documents retain a zero score without an API call; empty strings are scored.
API failures, mismatched answer IDs, and invalid probabilities raise errors.
Batching changes the payload and can affect model scores; compare quality and
latency on your workload before opting in.

::: lancedb.rerankers
    options:
      show_root_heading: false
      show_root_toc_entry: false

## Connections (Asynchronous)

::: lancedb.connect_catalog_async

::: lancedb.catalog.AsyncCatalog

::: lancedb.catalog.AsyncDatabaseNames

Connections represent a connection to a LanceDb database and
can be used to create, list, or open tables.

::: lancedb.connect_async

::: lancedb.db.AsyncConnection

::: lancedb.listing.AsyncListing

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
