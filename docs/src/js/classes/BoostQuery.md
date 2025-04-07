[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BoostQuery

# Class: BoostQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Implements

- [`FullTextQuery`](../interfaces/FullTextQuery.md)

## Constructors

### new BoostQuery()

```ts
new BoostQuery(
   positive,
   negative,
   negativeBoost): BoostQuery
```

Creates an instance of BoostQuery.

#### Parameters

* **positive**: [`FullTextQuery`](../interfaces/FullTextQuery.md)
    The positive query that boosts the relevance score.

* **negative**: [`FullTextQuery`](../interfaces/FullTextQuery.md)
    The negative query that reduces the relevance score.

* **negativeBoost**: `number`
    The factor by which the negative query reduces the score.

#### Returns

[`BoostQuery`](BoostQuery.md)

## Methods

### queryType()

```ts
queryType(): FullTextQueryType
```

#### Returns

[`FullTextQueryType`](../enumerations/FullTextQueryType.md)

#### Implementation of

[`FullTextQuery`](../interfaces/FullTextQuery.md).[`queryType`](../interfaces/FullTextQuery.md#querytype)

***

### toDict()

```ts
toDict(): Record<string, unknown>
```

#### Returns

`Record`&lt;`string`, `unknown`&gt;

#### Implementation of

[`FullTextQuery`](../interfaces/FullTextQuery.md).[`toDict`](../interfaces/FullTextQuery.md#todict)
