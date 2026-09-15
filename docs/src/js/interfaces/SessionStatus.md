[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / SessionStatus

# Interface: SessionStatus

Safe, non-secret view of a cached OAuth session, returned by
[OAuthSession.status](../classes/OAuthSession.md#status) and [OAuthSession.login](../classes/OAuthSession.md#login).

## Properties

### clientId

```ts
clientId: string;
```

Client ID of the cached session.

***

### flow

```ts
flow: string;
```

Flow that produced the cached session.

***

### issuerUrl

```ts
issuerUrl: string;
```

Canonical issuer URL of the cached session.

***

### obtainedAt?

```ts
optional obtainedAt: number;
```

When the cached session was obtained, as Unix seconds.

***

### refreshable

```ts
refreshable: boolean;
```

Whether a cached session exists that can obtain tokens without
interactive authentication. Because access tokens are not persisted,
this is `true` exactly when a refresh token is cached; the next
connection refreshes with it rather than opening a browser or device
prompt.

***

### scopes

```ts
scopes: string[];
```

Canonical (sorted, de-duplicated) scope set of the cached session.
