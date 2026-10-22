# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import pyarrow as pa
import pytest

# The metrics recorder is process-global and installed once, so the whole
# bridge is exercised in a single test to avoid cross-test global-state coupling.


def _metrics_by_name(reader):
    data = reader.get_metrics_data()
    result = {}
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                result[metric.name] = metric
    return result


def test_instrument_lancedb_metrics_exports_object_store_metrics(tmp_path):
    pytest.importorskip("opentelemetry.sdk.metrics")
    from lancedb.otel import instrument_lancedb_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    assert instrument_lancedb_metrics(provider)

    # The catalog is populated once the recorder is installed.
    from lancedb._lancedb import lancedb_metrics_catalog

    catalog = {desc.name: desc for desc in lancedb_metrics_catalog()}
    # Every metric kind emitted by the object store must be described so it is
    # surfaced by the bridge (counter, histogram, and gauge).
    assert catalog["lance_object_store_requests_total"].kind == "counter"
    assert catalog["lance_object_store_request_duration_seconds"].kind == "histogram"
    assert catalog["lance_object_store_in_flight_requests"].kind == "gauge"
    assert catalog["lance_object_store_retryable_responses_total"].kind == "counter"

    # Generate object store activity on the local filesystem (scheme "file").
    db = lancedb.connect(str(tmp_path))
    table = db.create_table("t", pa.table({"id": pa.array(range(256))}))
    assert table.count_rows() == 256
    assert table.to_arrow().num_rows == 256

    metrics = _metrics_by_name(reader)

    requests = metrics["lance_object_store_requests_total"]
    points = list(requests.data.data_points)
    assert points, "expected at least one request data point"
    # Object store metrics are labelled by `operation` and `base` (the store
    # scheme, e.g. "file", by default).
    assert all("base" in p.attributes and "operation" in p.attributes for p in points)
    assert sum(p.value for p in points) > 0

    # Histograms are decomposed into bucket / count / sum observable counters.
    bucket = metrics["lance_object_store_request_duration_seconds_bucket"]
    bucket_points = list(bucket.data.data_points)
    assert bucket_points
    assert all("le" in p.attributes for p in bucket_points)
    # The implicit +Inf bucket must be present and is the cumulative maximum.
    assert any(p.attributes["le"] == "+Inf" for p in bucket_points)

    count = metrics["lance_object_store_request_duration_seconds_count"]
    assert sum(p.value for p in count.data.data_points) > 0

    # The `_sum` instrument must also be wired and report positive latency.
    duration_sum = metrics["lance_object_store_request_duration_seconds_sum"]
    assert sum(p.value for p in duration_sum.data.data_points) > 0

    # Unit handling: only `_sum` keeps the histogram's unit (seconds); `_bucket`
    # and `_count` observe cumulative counts and are unitless.
    assert duration_sum.unit == "s"
    assert bucket.unit == ""
    assert count.unit == ""


def test_snapshot_empty_before_install_is_safe():
    # snapshot is callable regardless of installation state and never raises.
    from lancedb._lancedb import snapshot_lancedb_metrics

    assert isinstance(snapshot_lancedb_metrics(), list)


def test_instrument_warns_when_recorder_unavailable(monkeypatch):
    # A foreign `metrics` recorder already installed -> register returns False;
    # instrument_lancedb_metrics must warn and return False without instrumenting.
    pytest.importorskip("opentelemetry.sdk.metrics")
    import lancedb.otel as otel
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    monkeypatch.setattr(otel, "register_lancedb_metrics_recorder", lambda: False)

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    with pytest.warns(UserWarning, match="recorder"):
        assert otel.instrument_lancedb_metrics(provider) is False
