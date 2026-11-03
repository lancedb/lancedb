[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / SessionLogout

# Interface: SessionLogout

Result of [OAuthSession.logout](../classes/OAuthSession.md#logout).

## Properties

### removed

```ts
removed: boolean;
```

Whether a cached credential was removed. `false` means no matching
session was cached; logout is idempotent.
