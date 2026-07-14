# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Bridge LanceDB's internal metrics into OpenTelemetry.

LanceDB (through Lance core) publishes metrics (currently object store request
counts, bytes, latency, errors, and throttles) through the Rust ``metrics``
facade. This module installs a process-global recorder that aggregates them and
registers OpenTelemetry observable instruments that report the aggregated values
into the user's ``MeterProvider``.

The bridge is generic: every metric LanceDB describes is surfaced automatically,
with no per-metric Python code. Histograms have no asynchronous OpenTelemetry
instrument, so each is exported Prometheus-style as cumulative ``le`` buckets
plus ``_count`` and ``_sum`` observable counters.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Optional

from ._lancedb import (
    lancedb_metrics_catalog,
    register_lancedb_metrics_recorder,
    snapshot_lancedb_metrics,
)

if TYPE_CHECKING:
    from opentelemetry.metrics import MeterProvider

_INSTRUMENTED = False


def instrument_lancedb_metrics(
    meter_provider: Optional["MeterProvider"] = None,
) -> bool:
    """Register LanceDB metrics as OpenTelemetry observable instruments.

    Installs a process-global metrics recorder and creates one observable
    instrument per LanceDB metric on the given (or global) ``MeterProvider``. The
    user's configured ``MetricReader`` then collects them on its own schedule.

    Counters and gauges map directly to observable counters/gauges. Each
    histogram is exported as cumulative ``le`` bucket counts (``<name>_bucket``,
    with an ``le`` attribute) plus ``<name>_count`` and ``<name>_sum``.

    Parameters
    ----------
    meter_provider : opentelemetry.metrics.MeterProvider, optional
        The provider to register instruments on. Defaults to the global provider
        from ``opentelemetry.metrics.get_meter_provider()``.

    Returns
    -------
    bool
        ``True`` if the recorder is installed and instruments are registered.
        ``False`` if a different ``metrics`` recorder is already installed in
        this process (``metrics`` permits only one global recorder), in which
        case a warning is emitted and no instruments are created.

    Notes
    -----
    Requires the OpenTelemetry API (``pip install lancedb[otel]``) and, to
    actually export, an OpenTelemetry SDK (``pip install opentelemetry-sdk``)
    configured by the application. Calling this more than once is safe;
    instruments are created only on the first successful call.
    """
    global _INSTRUMENTED

    try:
        from opentelemetry.metrics import Observation, get_meter_provider
    except ImportError as exc:
        raise ImportError(
            "instrument_lancedb_metrics requires the OpenTelemetry API/SDK. "
            "Install it with `pip install lancedb[otel]` or "
            "`pip install opentelemetry-sdk`."
        ) from exc

    if not register_lancedb_metrics_recorder():
        warnings.warn(
            "Could not install the LanceDB metrics recorder: another `metrics` "
            "recorder is already installed in this process. LanceDB metrics will "
            "not be exported via OpenTelemetry.",
            stacklevel=2,
        )
        return False

    if _INSTRUMENTED:
        return True

    provider = meter_provider or get_meter_provider()
    meter = provider.get_meter("lancedb")

    def scalar_callback(metric_name: str):
        def callback(_options):
            return [
                Observation(point.value, point.attributes)
                for point in snapshot_lancedb_metrics()
                if point.name == metric_name and point.value is not None
            ]

        return callback

    def bucket_callback(metric_name: str):
        def callback(_options):
            observations = []
            for point in snapshot_lancedb_metrics():
                if point.name != metric_name or point.buckets is None:
                    continue
                for le, cumulative in point.buckets:
                    attributes = dict(point.attributes)
                    attributes["le"] = le
                    observations.append(Observation(cumulative, attributes))
            return observations

        return callback

    def field_callback(metric_name: str, field: str):
        def callback(_options):
            observations = []
            for point in snapshot_lancedb_metrics():
                if point.name != metric_name:
                    continue
                value = getattr(point, field)
                if value is not None:
                    observations.append(Observation(value, point.attributes))
            return observations

        return callback

    for desc in lancedb_metrics_catalog():
        unit = desc.unit or ""
        if desc.kind == "counter":
            meter.create_observable_counter(
                desc.name,
                callbacks=[scalar_callback(desc.name)],
                unit=unit,
                description=desc.description,
            )
        elif desc.kind == "gauge":
            meter.create_observable_gauge(
                desc.name,
                callbacks=[scalar_callback(desc.name)],
                unit=unit,
                description=desc.description,
            )
        elif desc.kind == "histogram":
            # `_bucket` and `_count` observe cumulative sample counts, not the
            # histogram's measured quantity, so they are unitless; only `_sum`
            # carries the histogram's unit.
            meter.create_observable_counter(
                f"{desc.name}_bucket",
                callbacks=[bucket_callback(desc.name)],
                description=f"{desc.description} (cumulative buckets)",
            )
            meter.create_observable_counter(
                f"{desc.name}_count",
                callbacks=[field_callback(desc.name, "count")],
                description=f"{desc.description} (count)",
            )
            meter.create_observable_counter(
                f"{desc.name}_sum",
                callbacks=[field_callback(desc.name, "sum")],
                unit=unit,
                description=f"{desc.description} (sum)",
            )

    _INSTRUMENTED = True
    return True
