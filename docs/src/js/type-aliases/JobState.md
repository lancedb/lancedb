[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / JobState

# Type Alias: JobState

```ts
type JobState:
  | "created"
  | "queued"
  | "in_progress"
  | "succeeded"
  | "failed"
  | "canceled"
  | "timed_out"
  | string & object;
```

Detailed lifecycle state. Unrecognized future values remain strings.
