[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / WriteExecutionOptions

# Interface: WriteExecutionOptions

## Properties

### timeoutMs?

```ts
optional timeoutMs: number;
```

Maximum time to run the operation before cancelling it.

By default, there is a 30-second timeout that is only enforced after the
first attempt. This is to prevent spending too long retrying to resolve
conflicts. For example, if a write attempt takes 20 seconds and fails,
the second attempt will be cancelled after 10 seconds, hitting the
30-second timeout. However, a write that takes one hour and succeeds on the
first attempt will not be cancelled.

When this is set, the timeout is enforced on all attempts, including the first.
