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

```typescript
const config: OAuthConfig = {
  issuerUrl: "https://login.microsoftonline.com/{tenant}/v2.0",
  clientId: "app-id",
  scopes: ["api://lancedb-api/.default"],
  flow: OAuthFlowType.AzureManagedIdentity,
};
```

## Properties

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

### refreshBufferSecs?

```ts
optional refreshBufferSecs: number;
```

Seconds before expiry to trigger proactive refresh (default: 300).
Keep this well below the token TTL; if it is greater than or equal to
the TTL, each request refreshes the token.

***

### scopes

```ts
scopes: string[];
```

OAuth scopes to request.
For Azure managed identity, exactly one scope or resource is required.
For example: `["api://{app_id}/.default"]`
