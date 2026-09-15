[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / NativeOAuthConfig

# Interface: NativeOAuthConfig

OAuth configuration for LanceDB authentication.

This is the generated napi-rs binding shape. TypeScript users should prefer
the public `OAuthConfig` type exported from `@lancedb/lancedb`.

All token acquisition and refresh is handled in the Rust layer.

## Properties

### audience?

```ts
optional audience: string;
```

Optional provider-specific audience for authorization and token requests.

***

### callbackPort?

```ts
optional callbackPort: number;
```

Port for the authorization_code loopback callback server.

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

Client secret (required for client_credentials).

***

### flow?

```ts
optional flow: string;
```

Authentication flow: "client_credentials", "authorization_code",
"device_code", or "azure_managed_identity"

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

Client ID for user-assigned managed identity (azure_managed_identity).

***

### redirectUri?

```ts
optional redirectUri: string;
```

Loopback redirect URI for authorization_code.

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

Optional resource indicator for authorization and token requests.

***

### scopes

```ts
scopes: string[];
```

OAuth scopes to request. For Azure managed identity, exactly one scope
or resource is required. For example: `["api://{app_id}/.default"]`

***

### tokenCache?

```ts
optional tokenCache: TokenCacheOptions;
```

Opt in to the persistent token cache so short-lived processes reuse
one session. Only refresh tokens are persisted.

***

### usePkce?

```ts
optional usePkce: boolean;
```

Whether authorization_code uses S256 PKCE (default: true).
