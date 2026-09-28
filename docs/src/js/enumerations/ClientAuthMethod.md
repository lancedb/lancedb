[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ClientAuthMethod

# Enumeration: ClientAuthMethod

How the client authenticates to the OAuth token endpoint.

The method applies to every OAuth request that carries client
authentication: client-credentials, authorization-code exchange,
refresh-token, and device-authorization requests. The Azure managed
identity flow ignores this option.

## Enumeration Members

### ClientSecretBasic

```ts
ClientSecretBasic: "client_secret_basic";
```

HTTP Basic authentication. This is the RFC 6749 recommended method and
the normal default for confidential clients, including default Okta
applications. Requires `clientSecret`.

***

### ClientSecretPost

```ts
ClientSecretPost: "client_secret_post";
```

Credentials in the request body, for providers configured to require it.
Requires `clientSecret`.

***

### None

```ts
None: "none";
```

No client authentication, for public clients using PKCE or the device
flow. Cannot be combined with `clientSecret`.
