[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / MatchQuery

# Class: MatchQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Implements

- [`FullTextQuery`](../interfaces/FullTextQuery.md)

## Constructors

### new MatchQuery()

```ts
new MatchQuery(
   query,
   column,
   options?): MatchQuery
```

Creates an instance of MatchQuery.

#### Parameters

* **query**: `string`
    The text query to search for.

* **column**: `string`
    The name of the column to search within.

* **options?**
    Optional parameters for the match query.
    - `boost`: The boost factor for the query (default is 1.0).
    - `fuzziness`: The fuzziness level for the query (default is 0).
    - `maxExpansions`: The maximum number of terms to consider for fuzzy matching (default is 50).
    - `operator`: The logical operator to use for combining terms in the query (default is "OR").

* **options.boost?**: `number`

* **options.fuzziness?**: `number`

* **options.maxExpansions?**: `number`

* **options.operator?**: [`Operator`](../enumerations/Operator.md)

#### Returns

[`MatchQuery`](MatchQuery.md)

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
