[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / CleanupOptions

# Interface: CleanupOptions

Per-run retention; omission inherits selected table/branch then deployment policy.

## Properties

### minAgeSeconds?

```ts
optional minAgeSeconds: number;
```

Minimum old-version age in seconds. Zero is explicit, not omitted.
