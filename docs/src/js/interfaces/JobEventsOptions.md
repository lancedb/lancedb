[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobEventsOptions

# Interface: JobEventsOptions

Which of a job's events [Job.events](../classes/Job.md#events) returns.

## Properties

### filter?

```ts
optional filter: string;
```

SQL-like filter over the event columns.

***

### limit?

```ts
optional limit: number;
```

Maximum event rows to return, up to the server maximum of 10,000.
