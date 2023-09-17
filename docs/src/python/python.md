# LanceDB Python API Reference

## Installation

```shell
pip install lancedb
```

## Connection

::: lancedb.connect

::: lancedb.db.DBConnection

## Table

::: lancedb.table.Table

## Querying

::: lancedb.query.Query

::: lancedb.query.LanceQueryBuilder

::: lancedb.query.LanceFtsQueryBuilder

## Embeddings

::: lancedb.embeddings.functions.EmbeddingFunctionRegistry

::: lancedb.embeddings.functions.EmbeddingFunction

::: lancedb.embeddings.functions.TextEmbeddingFunction

::: lancedb.embeddings.functions.SentenceTransformerEmbeddings

::: lancedb.embeddings.functions.OpenAIEmbeddings

::: lancedb.embeddings.functions.OpenClipEmbeddings

::: lancedb.embeddings.with_embeddings

## Context

::: lancedb.context.contextualize

::: lancedb.context.Contextualizer

## Full text search

::: lancedb.fts.create_index

::: lancedb.fts.populate_index

::: lancedb.fts.search_index

## Utilities

::: lancedb.vector

## Integrations

### Pydantic

::: lancedb.pydantic.pydantic_to_schema

::: lancedb.pydantic.vector

::: lancedb.pydantic.LanceModel
