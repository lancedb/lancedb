[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / NativeOAuthConfig

# Interface: NativeOAuthConfig

OAuth configuration for LanceDB authentication.

This is the generated napi-rs binding shape. TypeScript users should prefer
the public `OAuthConfig` type exported from `@lancedb/lancedb`.

All token acquisition and refresh is handled in the Rust layer.

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

Client secret (required for client_credentials).

***

### flow?

```ts
optional flow: string;
```

Authentication flow: "client_credentials" or "azure_managed_identity"

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

OAuth scopes to request. For Azure managed identity, exactly one scope
or resource is required. For example: `["api://{app_id}/.default"]`
