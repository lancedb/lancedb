[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / QueryJobEventsOptions

# Interface: QueryJobEventsOptions

Which events [Connection.queryJobEvents](../classes/Connection.md#queryjobevents) returns.

## Properties

### filter?

```ts
optional filter: string;
```

SQL-like filter over the event columns.

***

### jobId?

```ts
optional jobId: string;
```

Restrict to one job. Every job when omitted.

***

### limit?

```ts
optional limit: number;
```

Maximum event rows to return, up to the server maximum of 10,000.
