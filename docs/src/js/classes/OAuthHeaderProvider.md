[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OAuthHeaderProvider

# Class: OAuthHeaderProvider

Example implementation: OAuth token provider with automatic refresh.

This is an example implementation showing how to manage OAuth tokens
with automatic refresh when they expire.

## Example

```typescript
async function fetchToken(): Promise<TokenResponse> {
  const response = await fetch("https://oauth.example.com/token", {
    method: "POST",
    body: JSON.stringify({
      grant_type: "client_credentials",
      client_id: "your-client-id",
      client_secret: "your-client-secret"
    }),
    headers: { "Content-Type": "application/json" }
  });
  const data = await response.json();
  return {
    accessToken: data.access_token,
    expiresIn: data.expires_in
  };
}

const provider = new OAuthHeaderProvider(fetchToken);
const headers = provider.getHeaders();
// Returns: {"authorization": "Bearer <your-token>"}
```

## Extends

- [`HeaderProvider`](HeaderProvider.md)

## Constructors

### new OAuthHeaderProvider()

```ts
new OAuthHeaderProvider(tokenFetcher, refreshBufferSeconds): OAuthHeaderProvider
```

Initialize the OAuth provider.

#### Parameters

* **tokenFetcher**
    Function to fetch new tokens. Should return object with 'accessToken' and optionally 'expiresIn'.

* **refreshBufferSeconds**: `number` = `300`
    Seconds before expiry to refresh token. Default 300 (5 minutes).

#### Returns

[`OAuthHeaderProvider`](OAuthHeaderProvider.md)

#### Overrides

[`HeaderProvider`](HeaderProvider.md).[`constructor`](HeaderProvider.md#constructors)

## Methods

### getHeaders()

```ts
getHeaders(): Record<string, string>
```

Get OAuth headers, refreshing token if needed.
Note: This is synchronous for now as the Rust implementation expects sync.
In a real implementation, this would need to handle async properly.

#### Returns

`Record`&lt;`string`, `string`&gt;

Headers with Bearer token authorization.

#### Throws

If unable to fetch or refresh token.

#### Overrides

[`HeaderProvider`](HeaderProvider.md).[`getHeaders`](HeaderProvider.md#getheaders)

***

### refreshToken()

```ts
refreshToken(): Promise<void>
```

Manually refresh the token.
Call this before using getHeaders() to ensure token is available.

#### Returns

`Promise`&lt;`void`&gt;
