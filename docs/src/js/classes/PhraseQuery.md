[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / PhraseQuery

# Class: PhraseQuery

Represents a full-text query interface.
This interface defines the structure and behavior for full-text queries,
including methods to retrieve the query type and convert the query to a dictionary format.

## Implements

- [`FullTextQuery`](../interfaces/FullTextQuery.md)

## Constructors

### new PhraseQuery()

```ts
new PhraseQuery(
   query,
   column,
   options?): PhraseQuery
```

Creates an instance of `PhraseQuery`.

#### Parameters

* **query**: `string`
    The phrase to search for in the specified column.

* **column**: `string`
    The name of the column to search within.

* **options?**
    Optional parameters for the phrase query.
    - `slop`: The maximum number of intervening unmatched positions allowed between words in the phrase (default is 0).

* **options.slop?**: `number`

#### Returns

[`PhraseQuery`](PhraseQuery.md)

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
