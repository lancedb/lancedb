[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [rerankers](../README.md) / RRFReranker

# Class: RRFReranker

Reranks the results using the Reciprocal Rank Fusion (RRF) algorithm.

Internally this uses the Rust implementation

## Constructors

### new RRFReranker()

```ts
new RRFReranker(inner): RRFReranker
```

#### Parameters

* **inner**: `RrfReranker`

#### Returns

[`RRFReranker`](RRFReranker.md)

## Methods

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
