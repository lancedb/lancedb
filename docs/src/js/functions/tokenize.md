[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / tokenize

# Function: tokenize()

```ts
function tokenize(query, options?): Promise<FtsToken[]>
```

Tokenize a full-text search query using an explicit tokenizer.

This does not require a table or FTS index. The tokenizer options match
[Index.fts](../classes/Index.md#fts).

## Parameters

* **query**: `string`

* **options?**: `Partial`&lt;[`TokenizeOptions`](../interfaces/TokenizeOptions.md)&gt;

## Returns

`Promise`&lt;[`FtsToken`](../interfaces/FtsToken.md)[]&gt;
