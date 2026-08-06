[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [rerankers](../README.md) / RRFReranker

# Class: RRFReranker

Reranks the results using the Reciprocal Rank Fusion (RRF) algorithm.

## Methods

### outputSchema()

```ts
outputSchema(inputSchema): Promise<Schema<any>>
```

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

***

### create()

```ts
static create(k): Promise<RRFReranker>
```

#### Parameters

* **k**: `number` = `60`

#### Returns

`Promise`&lt;[`RRFReranker`](RRFReranker.md)&gt;
