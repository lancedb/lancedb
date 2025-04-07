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
new PhraseQuery(query, column): PhraseQuery
```

Creates an instance of `PhraseQuery`.

#### Parameters

* **query**: `string`
    The phrase to search for in the specified column.

* **column**: `string`
    The name of the column to search within.

#### Returns

[`PhraseQuery`](PhraseQuery.md)

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
