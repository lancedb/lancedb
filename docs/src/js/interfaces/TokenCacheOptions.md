[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / TokenCacheOptions

# Interface: TokenCacheOptions

Options for the persistent OAuth token cache.

The cache is opt-in: it is only used when set as `tokenCache` on
[OAuthConfig](OAuthConfig.md). Only refresh tokens are persisted, in a private
directory with owner-only permissions, so short-lived processes can reuse
an authenticated session instead of re-prompting on every start.

Multiple identities (issuer, client, scopes, resource, audience, flow, client authentication)
get separate cache entries. Within one identity the most recent login wins.

## Properties

### cacheDir?

```ts
optional cacheDir: string;
```

Directory that holds cached credentials. Defaults to
`$XDG_CACHE_HOME/lancedb/oauth`, `$HOME/.cache/lancedb/oauth` on Unix,
or `%LOCALAPPDATA%\\lancedb\\oauth` on Windows. The directory is created
with owner-only permissions (`0700`) when missing.

***

### lockTimeoutSecs?

```ts
optional lockTimeoutSecs: number;
```

How long to wait for the cross-process refresh lock before failing, in
seconds (default: 30).
