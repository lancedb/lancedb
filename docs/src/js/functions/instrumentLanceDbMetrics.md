[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / instrumentLanceDbMetrics

# Function: instrumentLanceDbMetrics()

```ts
function instrumentLanceDbMetrics(meterProvider?): boolean
```

Register LanceDB metrics as OpenTelemetry observable instruments.

Installs a process-global metrics recorder and creates one observable
instrument per LanceDB metric (currently object store request counts, bytes,
latency, errors, and throttles) on the given (or global) `MeterProvider`. The
configured `MetricReader` then collects them on its own schedule.

Counters and gauges map directly to observable counters/gauges. Because
OpenTelemetry has no asynchronous histogram instrument, each histogram is
exported Prometheus-style as cumulative `le` bucket counts (`<name>_bucket`,
with an `le` attribute) plus `<name>_count` and `<name>_sum`.

Requires `@opentelemetry/api` (a dependency) and, to actually export, an
OpenTelemetry SDK such as `@opentelemetry/sdk-metrics`.

## Parameters

* **meterProvider?**: `MeterProvider`
    The provider to register instruments on. Defaults to the
    global provider from `@opentelemetry/api`.

## Returns

`boolean`

`true` if the recorder is installed and instruments are registered.
  `false` if a different `metrics` recorder is already installed in this
  process (only one global recorder is permitted), in which case a warning is
  emitted and no instruments are created. Calling this more than once is safe;
  instruments are created only on the first successful call.
