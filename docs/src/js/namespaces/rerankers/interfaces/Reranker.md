[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [rerankers](../README.md) / Reranker

# Interface: Reranker

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
