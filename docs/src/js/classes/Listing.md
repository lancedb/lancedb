[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / Listing

# Class: Listing&lt;T&gt;

A lazy async iterator over database resources with pagination state.
Requests occur during iteration; request errors terminate the iterator.

## Example

```ts
for await (const name of connection.listViews([], { pageLimit: 20 })) {
  console.log(name);
}
```

## Type Parameters

• **T**

## Implements

- `AsyncIterableIterator`&lt;`T`&gt;

## Methods

### \[asyncIterator\]()

```ts
asyncIterator: AsyncIterableIterator<T>
```

#### Returns

`AsyncIterableIterator`&lt;`T`&gt;

#### Implementation of

`AsyncIterableIterator.[asyncIterator]`

***

### next()

```ts
next(): Promise<IteratorResult<T, any>>
```

Fetch the next item, requesting another page only when needed.

#### Returns

`Promise`&lt;`IteratorResult`&lt;`T`, `any`&gt;&gt;

#### Implementation of

`AsyncIterableIterator.next`

***

### numPageResults()

```ts
numPageResults(): number
```

Number of cached items available without another request.

#### Returns

`number`

***

### pageToken()

```ts
pageToken(): undefined | string
```

Token for the next request. Initially this is the supplied starting token.
Returns undefined after fetching the final page, even with cached items.
Drain the cache before saving a token to avoid skipping items on resumption.
Failed requests terminate iteration and retain their token for a new iterator.

#### Returns

`undefined` \| `string`
