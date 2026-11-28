[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionRecompute

# Type Alias: FunctionRecompute

```ts
type FunctionRecompute: "failed" | "all" | "none";
```

Which existing values to recompute during a Function version upgrade.
`"failed"` selects current failures. `"all"` refreshes every eligible live
row. `"none"` preserves existing values and only changes the binding.
