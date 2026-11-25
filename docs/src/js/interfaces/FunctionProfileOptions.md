[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / FunctionProfileOptions

# Interface: FunctionProfileOptions

Options for requesting a Function worker capture.

## Properties

### durationSeconds?

```ts
optional durationSeconds: number;
```

1–30 seconds for CPU/memory, default 10; 1–5 for GPU, default 5.

***

### kind?

```ts
optional kind: FunctionProfileKind;
```

CPU samples, retained Python allocations, or PyTorch CUDA activities.

***

### sessionSlot?

```ts
optional sessionSlot: number;
```

Python execution session within the worker; default zero.
