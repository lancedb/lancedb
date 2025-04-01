[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FullTextQuery

# Interface: FullTextQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Methods

### queryType()

```ts
queryType(): FullTextQueryType
```

#### Returns

[`FullTextQueryType`](../enumerations/FullTextQueryType.md)

***

### toDict()

```ts
toDict(): Record<string, unknown>
```

#### Returns

`Record`&lt;`string`, `unknown`&gt;
