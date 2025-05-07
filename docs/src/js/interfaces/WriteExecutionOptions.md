[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / WriteExecutionOptions

# Interface: WriteExecutionOptions

## Properties

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum time (in milliseconds) to run the operation before cancelling it.
Default is no timeout for first attempt, but an overall timeout of
30 seconds is applied after that.
