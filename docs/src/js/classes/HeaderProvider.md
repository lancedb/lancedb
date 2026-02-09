[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / HeaderProvider

# Class: `abstract` HeaderProvider

Abstract base class for providing custom headers for each request.

Users can implement this interface to provide dynamic headers for various purposes
such as authentication (OAuth tokens, API keys), request tracking (correlation IDs),
custom metadata, or any other header-based requirements. The provider is called
before each request to ensure fresh header values are always used.

## Examples

Simple JWT token provider:
```typescript
class JWTProvider extends HeaderProvider {
  constructor(private token: string) {
    super();
  }

  getHeaders(): Record<string, string> {
    return { authorization: `Bearer ${this.token}` };
  }
}
```

Provider with request tracking:
```typescript
class RequestTrackingProvider extends HeaderProvider {
  constructor(private sessionId: string) {
    super();
  }

  getHeaders(): Record<string, string> {
    return {
      "X-Session-Id": this.sessionId,
      "X-Request-Id": `req-${Date.now()}`
    };
  }
}
```

## Extended by

- [`StaticHeaderProvider`](StaticHeaderProvider.md)
- [`OAuthHeaderProvider`](OAuthHeaderProvider.md)

## Constructors

### new HeaderProvider()

```ts
new HeaderProvider(): HeaderProvider
```

#### Returns

[`HeaderProvider`](HeaderProvider.md)

## Methods

### getHeaders()

```ts
abstract getHeaders(): Record<string, string>
```

Get the latest headers to be added to requests.

This method is called before each request to the remote LanceDB server.
Implementations should return headers that will be merged with existing headers.

#### Returns

`Record`&lt;`string`, `string`&gt;

Dictionary of header names to values to add to the request.

#### Throws

If unable to fetch headers, the exception will be propagated and the request will fail.
