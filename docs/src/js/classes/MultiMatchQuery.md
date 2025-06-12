[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MultiMatchQuery

# Class: MultiMatchQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Implements

- [`FullTextQuery`](../interfaces/FullTextQuery.md)

## Constructors

### new MultiMatchQuery()

```ts
new MultiMatchQuery(
   query,
   columns,
   options?): MultiMatchQuery
```

Creates an instance of MultiMatchQuery.

#### Parameters

* **query**: `string`
    The text query to search for across multiple columns.

* **columns**: `string`[]
    An array of column names to search within.

* **options?**
    Optional parameters for the multi-match query.
    - `boosts`: An array of boost factors for each column (default is 1.0 for all).
    - `operator`: The logical operator to use for combining terms in the query (default is "OR").

* **options.boosts?**: `number`[]

* **options.operator?**: [`Operator`](../enumerations/Operator.md)

#### Returns

[`MultiMatchQuery`](MultiMatchQuery.md)

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
