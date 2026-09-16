[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / OAuthSession

# Class: OAuthSession

Explicit OAuth session lifecycle for the persistent token cache: eager
`login`, non-secret `status`, and local `logout`.

A session is built from the same [OAuthConfig](../interfaces/OAuthConfig.md) used to connect
(including its `tokenCache` options). A connection created with the same
configuration shares the cache, so logging in here prepares tokens for
later processes without any database request.

`login` always runs the configured interactive flow and replaces the cached
session (the most recent login wins). `logout` removes only the local
credential; it does not revoke anything with the provider and does not sign
out of a browser SSO session.

## Example

```typescript
const config: OAuthConfig = {
  issuerUrl: "https://issuer.example.com",
  clientId: "my-app",
  scopes: ["openid", "offline_access"],
  flow: OAuthFlowType.DeviceCode,
  tokenCache: { cacheDir: "/tmp/my-app/oauth-cache" },
};
const session = new OAuthSession(config);
const status = await session.login();
```

## Constructors

### new OAuthSession()

```ts
new OAuthSession(config): OAuthSession
```

Create a session manager for the given OAuth configuration.

#### Parameters

* **config**: [`OAuthConfig`](../interfaces/OAuthConfig.md)

#### Returns

[`OAuthSession`](OAuthSession.md)

## Methods

### login()

```ts
login(): Promise<SessionStatus>
```

Eagerly run the configured authentication flow and store the session.

A successful login always replaces any prior cached session for this
identity; if the provider does not issue a refresh token (for example
without `offline_access`), the previous record is removed and the status
reports `refreshable == false`.

#### Returns

`Promise`&lt;[`SessionStatus`](../interfaces/SessionStatus.md)&gt;

***

### logout()

```ts
logout(): Promise<SessionLogout>
```

Remove the matching local cached credential.

This only deletes the local cache entry. It does not revoke the refresh
token with the provider and does not sign out of a browser SSO session.
Repeated calls succeed; `removed` reports whether a credential existed.

#### Returns

`Promise`&lt;[`SessionLogout`](../interfaces/SessionLogout.md)&gt;

***

### status()

```ts
status(): Promise<SessionStatus>
```

Report whether a matching cached session exists, with safe metadata.

This never contacts the identity provider and never exposes token values.

#### Returns

`Promise`&lt;[`SessionStatus`](../interfaces/SessionStatus.md)&gt;
