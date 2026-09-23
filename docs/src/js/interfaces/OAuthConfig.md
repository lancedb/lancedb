[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OAuthConfig

# Interface: OAuthConfig

OAuth configuration for LanceDB authentication.

This is the public TypeScript OAuth configuration type. The generated
`NativeOAuthConfig` type has the same runtime shape but is an implementation
detail of the napi-rs binding.

All token acquisition and refresh is handled in the Rust layer.
This config is passed through to Rust via napi-rs.

## Examples

```typescript
const config: OAuthConfig = {
  issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
  clientId: "app-id",
  clientSecret: "secret",
  scopes: ["api://lancedb-api/.default"],
};
```

Providers requiring an explicit target can set `resource` and/or `audience`:
```typescript
const targeted: OAuthConfig = {
  issuerUrl: "https://issuer.example.com",
  clientId: "app-id",
  clientSecret: "secret",
  scopes: ["read"],
  resource: "https://api.example.com",
  audience: "lancedb-api",
};
```

```typescript
const config: OAuthConfig = {
  issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
  clientId: "app-id",
  scopes: ["api://lancedb-api/.default"],
  flow: OAuthFlowType.AzureManagedIdentity,
};
```

The authorization URL is written to stderr before LanceDB tries to open a
browser, so it can be copied in headless environments.
```typescript
const config: OAuthConfig = {
  issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
  clientId: "app-id",
  scopes: ["openid", "api://lancedb-api/access"],
  flow: OAuthFlowType.AuthorizationCode,
};
```

Device Authorization writes the verification URL and user code to stderr
before polling begins.

## Properties

### audience?

```ts
optional audience: string;
```

Provider-specific audience, forwarded to authorization and token endpoints,
including refresh requests. Not supported for Azure managed identity.

***

### callbackPort?

```ts
optional callbackPort: number;
```

Port for the AuthorizationCode loopback callback server (default: 8400).

***

### clientAuthMethod?

```ts
optional clientAuthMethod: ClientAuthMethod;
```

How the client authenticates to the token endpoint (default: auto).
With a `clientSecret` the default is `ClientAuthMethod.ClientSecretBasic`,
which matches the RFC 6749 recommendation and the default configuration
of Okta confidential applications; without a secret the client is public
and no client authentication is sent.

***

### clientId

```ts
clientId: string;
```

Application / Client ID.

***

### clientSecret?

```ts
optional clientSecret: string;
```

Client secret (required for ClientCredentials).

***

### flow?

```ts
optional flow: OAuthFlowType;
```

Authentication flow (default: ClientCredentials).

***

### issuerUrl

```ts
issuerUrl: string;
```

OIDC issuer URL or OAuth authority URL.
For Azure: `https://login.microsoftonline.com/{tenant_id}/v2.0`

***

### managedIdentityClientId?

```ts
optional managedIdentityClientId: string;
```

Client ID for user-assigned managed identity (AzureManagedIdentity).

***

### redirectUri?

```ts
optional redirectUri: string;
```

Loopback redirect URI for AuthorizationCode.

***

### refreshBufferSecs?

```ts
optional refreshBufferSecs: number;
```

Seconds before expiry to trigger proactive refresh (default: 300).
Keep this well below the token TTL; if it is greater than or equal to
the TTL, each request refreshes the token.

***

### resource?

```ts
optional resource: string;
```

Resource indicator (RFC 8707), forwarded verbatim to authorization and token
endpoints, including refresh requests. Must be an absolute URI without a
fragment. Not supported for Azure managed identity.

***

### scopes

```ts
scopes: string[];
```

OAuth scopes to request.
For Azure managed identity, exactly one scope or resource is required.
For example: `["api://{app_id}/.default"]`

***

### tokenCache?

```ts
optional tokenCache: TokenCacheOptions;
```

Opt in to the persistent token cache so short-lived processes reuse one
session. Only refresh tokens are persisted. Only supported by
AuthorizationCode and DeviceCode; Azure managed identity is rejected.
Default: unset (memory only).

***

### usePkce?

```ts
optional usePkce: boolean;
```

Protect AuthorizationCode with S256 PKCE (default: true).
