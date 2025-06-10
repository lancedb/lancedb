[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / BooleanQuery

# Class: BooleanQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Implements

- [`FullTextQuery`](../interfaces/FullTextQuery.md)

## Constructors

### new BooleanQuery()

```ts
new BooleanQuery(queries): BooleanQuery
```

Creates an instance of BooleanQuery.

#### Parameters

* **queries**: [[`Occur`](../enumerations/Occur.md), [`FullTextQuery`](../interfaces/FullTextQuery.md)][]
    An array of (Occur, FullTextQuery objects) to combine.
    Occur specifies whether the query must match, or should match.

#### Returns

[`BooleanQuery`](BooleanQuery.md)

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
