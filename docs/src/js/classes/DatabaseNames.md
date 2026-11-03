[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / DatabaseNames

# Class: DatabaseNames

A lazy async iterator of database names with pagination state.
Returned by [Catalog.listDatabases](Catalog.md#listdatabases).

## Implements

- `AsyncIterableIterator`&lt;`string`&gt;

## Methods

### \[asyncIterator\]()

```ts
asyncIterator: AsyncIterableIterator<string>
```

#### Returns

`AsyncIterableIterator`&lt;`string`&gt;

#### Implementation of

`AsyncIterableIterator.[asyncIterator]`

***

### next()

```ts
next(): Promise<IteratorResult<string, any>>
```

Fetch the next name, requesting another page only when needed.

#### Returns

`Promise`&lt;`IteratorResult`&lt;`string`, `any`&gt;&gt;

#### Implementation of

`AsyncIterableIterator.next`

***

### numPageResults()

```ts
numPageResults(): number
```

Number of names available without another REST request.

#### Returns

`number`

***

### pageToken()

```ts
pageToken(): undefined | string
```

Token for the next REST request. Initially this is the supplied starting token.
Returns undefined after the final page is fetched, even if names remain cached.
Drain the cache before saving a token to avoid skipping names when resuming.
A failed request terminates iteration and retains its token for a new iterator.

#### Returns

`undefined` \| `string`
