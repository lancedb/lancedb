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
   boosts): MultiMatchQuery
```

Creates an instance of MultiMatchQuery.

#### Parameters

* **query**: `string`
    The text query to search for across multiple columns.

* **columns**: `string`[]
    An array of column names to search within.

* **boosts**: `number`[] = `...`
    (Optional) An array of boost factors corresponding to each column. Default is an array of 1.0 for each column.
    The `boosts` array should have the same length as `columns`. If not provided, all columns will have a default boost of 1.0.
    If the length of `boosts` is less than `columns`, it will be padded with 1.0s.

#### Returns

[`MultiMatchQuery`](MultiMatchQuery.md)

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
