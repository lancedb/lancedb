[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / ListingOptions

# Interface: ListingOptions

Pagination controls for lazy resource listings.

## Properties

### pageLimit?

```ts
optional pageLimit: number;
```

Maximum results per request, not a limit on the whole listing.

***

### pageToken?

```ts
optional pageToken: string;
```

Starting continuation token; omitted starts at the beginning.
