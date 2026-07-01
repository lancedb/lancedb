// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  MeterProvider,
  type MetricData,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import * as tmp from "tmp";
import {
  connect,
  instrumentLanceDbMetrics,
  snapshotLancedbMetrics,
} from "../lancedb";

// The metrics recorder is process-global and installed once, so the whole
// bridge is exercised in a single test to avoid cross-test global-state coupling.

// A minimal pull-based reader whose `collect()` we drive directly, invoking the
// observable-instrument callbacks. `@opentelemetry/sdk-metrics` ships no
// in-memory reader, so we subclass the abstract base.
class TestMetricReader extends MetricReader {
  protected async onForceFlush(): Promise<void> {
    // no-op: collection is driven directly via collect()
  }
  protected async onShutdown(): Promise<void> {
    // no-op: nothing to release
  }
}

async function metricsByName(
  reader: TestMetricReader,
): Promise<Map<string, MetricData>> {
  const collected = await reader.collect();
  const result = new Map<string, MetricData>();
  for (const scope of collected.resourceMetrics.scopeMetrics) {
    for (const metric of scope.metrics) {
      result.set(metric.descriptor.name, metric);
    }
  }
  return result;
}

describe("OpenTelemetry metrics bridge", () => {
  let tmpDir: tmp.DirResult;
  beforeEach(() => {
    tmpDir = tmp.dirSync({ unsafeCleanup: true });
  });
  afterEach(() => tmpDir.removeCallback());

  it("snapshot is safe to call regardless of install state", () => {
    expect(Array.isArray(snapshotLancedbMetrics())).toBe(true);
  });

  it("exports object store metrics via observable instruments", async () => {
    const reader = new TestMetricReader();
    const provider = new MeterProvider({ readers: [reader] });
    expect(instrumentLanceDbMetrics(provider)).toBe(true);

    // Generate object store activity on the local filesystem (scheme "file").
    const db = await connect(tmpDir.name);
    const data = Array.from({ length: 256 }, (_, i) => ({ id: i }));
    const table = await db.createTable("t", data);
    expect(await table.countRows()).toBe(256);

    const metrics = await metricsByName(reader);

    const requests = metrics.get("lance_object_store_requests_total");
    expect(requests).toBeDefined();
    // biome-ignore lint/suspicious/noExplicitAny: SDK point shape
    const requestPoints = (requests!.dataPoints as any[]) ?? [];
    expect(requestPoints.length).toBeGreaterThan(0);
    for (const p of requestPoints) {
      expect(p.attributes).toHaveProperty("scheme");
      expect(p.attributes).toHaveProperty("operation");
    }
    const totalRequests = requestPoints.reduce((acc, p) => acc + p.value, 0);
    expect(totalRequests).toBeGreaterThan(0);

    // Histograms are decomposed into bucket / count / sum observable counters.
    const bucket = metrics.get(
      "lance_object_store_request_duration_seconds_bucket",
    );
    expect(bucket).toBeDefined();
    // biome-ignore lint/suspicious/noExplicitAny: SDK point shape
    const bucketPoints = (bucket!.dataPoints as any[]) ?? [];
    expect(bucketPoints.length).toBeGreaterThan(0);
    expect(bucketPoints.every((p) => "le" in p.attributes)).toBe(true);
    // The implicit +Inf bucket must be present.
    expect(bucketPoints.some((p) => p.attributes.le === "+Inf")).toBe(true);

    const count = metrics.get(
      "lance_object_store_request_duration_seconds_count",
    );
    expect(count).toBeDefined();
    // biome-ignore lint/suspicious/noExplicitAny: SDK point shape
    const countPoints = (count!.dataPoints as any[]) ?? [];
    expect(countPoints.reduce((acc, p) => acc + p.value, 0)).toBeGreaterThan(0);

    const sum = metrics.get("lance_object_store_request_duration_seconds_sum");
    expect(sum).toBeDefined();
    // biome-ignore lint/suspicious/noExplicitAny: SDK point shape
    const sumPoints = (sum!.dataPoints as any[]) ?? [];
    expect(sumPoints.reduce((acc, p) => acc + p.value, 0)).toBeGreaterThan(0);

    await provider.shutdown();
  });
});
