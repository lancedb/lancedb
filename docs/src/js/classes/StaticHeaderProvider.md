[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / StaticHeaderProvider

# Class: StaticHeaderProvider

Example implementation: A simple header provider that returns static headers.

This is an example implementation showing how to create a HeaderProvider
for cases where headers don't change during the session.

## Example

```typescript
const provider = new StaticHeaderProvider({
  authorization: "Bearer my-token",
  "X-Custom-Header": "custom-value"
});
const headers = provider.getHeaders();
// Returns: {authorization: 'Bearer my-token', 'X-Custom-Header': 'custom-value'}
```

## Extends

- [`HeaderProvider`](HeaderProvider.md)

## Constructors

### new StaticHeaderProvider()

```ts
new StaticHeaderProvider(headers): StaticHeaderProvider
```

Initialize with static headers.

#### Parameters

* **headers**: `Record`&lt;`string`, `string`&gt;
    Headers to return for every request.

#### Returns

[`StaticHeaderProvider`](StaticHeaderProvider.md)

#### Overrides

[`HeaderProvider`](HeaderProvider.md).[`constructor`](HeaderProvider.md#constructors)

## Methods

### getHeaders()

```ts
getHeaders(): Record<string, string>
```

Return the static headers.

#### Returns

`Record`&lt;`string`, `string`&gt;

Copy of the static headers.

#### Overrides

[`HeaderProvider`](HeaderProvider.md).[`getHeaders`](HeaderProvider.md#getheaders)
