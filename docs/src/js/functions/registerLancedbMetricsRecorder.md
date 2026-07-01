[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / registerLancedbMetricsRecorder

# Function: registerLancedbMetricsRecorder()

```ts
function registerLancedbMetricsRecorder(): boolean
```

Install the LanceDB metrics recorder as the process-global `metrics` recorder.

Returns `true` if the recorder is installed (now or previously). Returns
`false` if a *different* recorder is already installed — `metrics` allows
only one global recorder per process, so LanceDB cannot coexist with another.

## Returns

`boolean`
