// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  type Attributes,
  type MeterProvider,
  type ObservableResult,
  metrics,
} from "@opentelemetry/api";

import {
  lancedbMetricsCatalog,
  registerLancedbMetricsRecorder,
  snapshotLancedbMetrics,
} from "./native";

let instrumented = false;

/**
 * Register LanceDB metrics as OpenTelemetry observable instruments.
 *
 * Installs a process-global metrics recorder and creates one observable
 * instrument per LanceDB metric (currently object store request counts, bytes,
 * latency, errors, and throttles) on the given (or global) `MeterProvider`. The
 * configured `MetricReader` then collects them on its own schedule.
 *
 * Counters and gauges map directly to observable counters/gauges. Because
 * OpenTelemetry has no asynchronous histogram instrument, each histogram is
 * exported Prometheus-style as cumulative `le` bucket counts (`<name>_bucket`,
 * with an `le` attribute) plus `<name>_count` and `<name>_sum`.
 *
 * Requires `@opentelemetry/api` (a dependency) and, to actually export, an
 * OpenTelemetry SDK such as `@opentelemetry/sdk-metrics`.
 *
 * @param meterProvider The provider to register instruments on. Defaults to the
 *   global provider from `@opentelemetry/api`.
 * @returns `true` if the recorder is installed and instruments are registered.
 *   `false` if a different `metrics` recorder is already installed in this
 *   process (only one global recorder is permitted), in which case a warning is
 *   emitted and no instruments are created. Calling this more than once is safe;
 *   instruments are created only on the first successful call.
 */
export function instrumentLanceDbMetrics(
  meterProvider?: MeterProvider,
): boolean {
  if (!registerLancedbMetricsRecorder()) {
    console.warn(
      "Could not install the LanceDB metrics recorder: another `metrics` " +
        "recorder is already installed in this process. LanceDB metrics will " +
        "not be exported via OpenTelemetry.",
    );
    return false;
  }

  if (instrumented) {
    return true;
  }

  const provider = meterProvider ?? metrics.getMeterProvider();
  const meter = provider.getMeter("lancedb");

  const scalarCallback = (metricName: string) => (result: ObservableResult) => {
    for (const point of snapshotLancedbMetrics()) {
      if (point.name === metricName && point.value != null) {
        result.observe(point.value, point.attributes);
      }
    }
  };

  const bucketCallback = (metricName: string) => (result: ObservableResult) => {
    for (const point of snapshotLancedbMetrics()) {
      if (point.name !== metricName || point.buckets == null) {
        continue;
      }
      for (const bucket of point.buckets) {
        const attributes: Attributes = {
          ...point.attributes,
          le: bucket.le,
        };
        result.observe(bucket.cumulativeCount, attributes);
      }
    }
  };

  const fieldCallback =
    (metricName: string, field: "count" | "sum") =>
    (result: ObservableResult) => {
      for (const point of snapshotLancedbMetrics()) {
        if (point.name !== metricName) {
          continue;
        }
        const value = point[field];
        if (value != null) {
          result.observe(value, point.attributes);
        }
      }
    };

  for (const desc of lancedbMetricsCatalog()) {
    const unit = desc.unit ?? "";
    if (desc.kind === "counter") {
      const counter = meter.createObservableCounter(desc.name, {
        unit,
        description: desc.description,
      });
      counter.addCallback(scalarCallback(desc.name));
    } else if (desc.kind === "gauge") {
      const gauge = meter.createObservableGauge(desc.name, {
        unit,
        description: desc.description,
      });
      gauge.addCallback(scalarCallback(desc.name));
    } else if (desc.kind === "histogram") {
      // `_bucket` and `_count` observe cumulative sample counts, not the
      // histogram's measured quantity, so they are unitless; only `_sum`
      // carries the histogram's unit.
      const bucket = meter.createObservableCounter(`${desc.name}_bucket`, {
        description: `${desc.description} (cumulative buckets)`,
      });
      bucket.addCallback(bucketCallback(desc.name));

      const count = meter.createObservableCounter(`${desc.name}_count`, {
        description: `${desc.description} (count)`,
      });
      count.addCallback(fieldCallback(desc.name, "count"));

      const sum = meter.createObservableCounter(`${desc.name}_sum`, {
        unit,
        description: `${desc.description} (sum)`,
      });
      sum.addCallback(fieldCallback(desc.name, "sum"));
    }
  }

  instrumented = true;
  return true;
}
