[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [rerankers](../README.md) / Reranker

# Interface: Reranker

## Methods

### outputSchema()?

```ts
optional outputSchema(inputSchema): Promise<Schema<any>>
```

Declare the schema returned when reranking a vector-only query.

This is required for vector-only reranking so query schema introspection
and execution agree. Hybrid-only rerankers may omit it.

#### Parameters

* **inputSchema**: `Schema`&lt;`any`&gt;

#### Returns

`Promise`&lt;`Schema`&lt;`any`&gt;&gt;

***

### rerankHybrid()

```ts
rerankHybrid(
   query,
   vecResults,
   ftsResults): Promise<RecordBatch<any>>
```

#### Parameters

* **query**: `string`

* **vecResults**: `RecordBatch`&lt;`any`&gt;

* **ftsResults**: `RecordBatch`&lt;`any`&gt;

#### Returns

`Promise`&lt;`RecordBatch`&lt;`any`&gt;&gt;
