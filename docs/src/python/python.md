# Python API Reference

This section contains the API reference for the Python API. There is a
synchronous and an asynchronous API client.

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

## Context

::: lancedb.context.contextualize

::: lancedb.context.Contextualizer

## Full text search

::: lancedb.fts.create_index

::: lancedb.fts.populate_index

::: lancedb.fts.search_index

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
