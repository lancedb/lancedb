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
   boost,
   fuzziness,
   maxExpansions): MatchQuery
```

Creates an instance of MatchQuery.

#### Parameters

* **query**: `string`
    The text query to search for.

* **column**: `string`
    The name of the column to search within.

* **boost**: `number` = `1.0`
    (Optional) The boost factor to influence the relevance score of this query. Default is `1.0`.

* **fuzziness**: `number` = `0`
    (Optional) The allowed edit distance for fuzzy matching. Default is `0`.

* **maxExpansions**: `number` = `50`
    (Optional) The maximum number of terms to consider for fuzzy matching. Default is `50`.

#### Returns

[`MatchQuery`](MatchQuery.md)

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
