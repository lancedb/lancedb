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
   options?): BoostQuery
```

Creates an instance of BoostQuery.

#### Parameters

* **positive**: [`FullTextQuery`](../interfaces/FullTextQuery.md)
    The positive query that boosts the relevance score.

* **negative**: [`FullTextQuery`](../interfaces/FullTextQuery.md)
    The negative query that reduces the relevance score.

* **options?**
    Optional parameters for the boost query.
    - `negativeBoost`: The boost factor for the negative query (default is 0.0).

* **options.negativeBoost?**: `number`

#### Returns

[`BoostQuery`](BoostQuery.md)

## Methods

### queryType()

```ts
queryType(): FullTextQueryType
```

The type of the full-text query.

#### Returns

[`FullTextQueryType`](../enumerations/FullTextQueryType.md)

#### Implementation of

[`FullTextQuery`](../interfaces/FullTextQuery.md).[`queryType`](../interfaces/FullTextQuery.md#querytype)
