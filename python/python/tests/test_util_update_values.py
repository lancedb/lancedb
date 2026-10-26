# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import date, datetime

import lancedb
import numpy as np
import pyarrow as pa
import pytest


ARRAY_VALUES = [
    ((1.0, 2.0), [1.0, 2.0]),
    ([1.0, 2.0], [1.0, 2.0]),
    (np.array([1.0, 2.0]), [1.0, 2.0]),
    (None, None),
]


@pytest.mark.parametrize("value,expected", ARRAY_VALUES)
def test_update_array_values(tmp_path, value, expected):
    dtype = pa.list_(pa.float64())
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "values", pa.table({"array": pa.array([[0.0, 0.0]], type=dtype)})
    )
    table.update(values={"array": value})
    assert table.to_arrow()["array"].to_pylist() == [expected]


@pytest.mark.asyncio
@pytest.mark.parametrize("value,expected", ARRAY_VALUES)
async def test_update_array_values_async(tmp_path, value, expected):
    dtype = pa.list_(pa.float64())
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table(
        "values", pa.table({"array": pa.array([[0.0, 0.0]], type=dtype)})
    )
    await table.update({"array": value})
    assert (await table.to_arrow())["array"].to_pylist() == [expected]


TIME_VALUES = [
    np.datetime64("2025", "Y"),
    np.datetime64("2025-01", "M"),
    np.datetime64("2025-01-02", "D"),
    np.datetime64("2025-01-02T03", "h"),
    np.datetime64("2025-01-02T03:04", "m"),
    np.datetime64("2025-01-02T03:04:05", "s"),
    np.datetime64("2025-01-02T03:04:05.123", "ms"),
    np.datetime64("2025-01-02T03:04:05.123456", "us"),
    np.datetime64("2025-01-02T03:04:05.123456789", "ns"),
    np.datetime64("NaT", "ns"),
    datetime(2025, 1, 2, 3, 4, 5, 123456),
    None,
]


def expected_time(value):
    if value is None or isinstance(value, np.datetime64) and np.isnat(value):
        return None
    return int(np.datetime64(value, "ns").astype(np.int64))


@pytest.mark.parametrize("value", TIME_VALUES)
def test_update_timestamp_values(tmp_path, value):
    db = lancedb.connect(tmp_path)
    data = pa.table({"time": pa.array([datetime(2025, 1, 1)], type=pa.timestamp("ns"))})
    table = db.create_table("times", data)
    table.update(values={"time": value})
    assert table.to_arrow()["time"].cast(pa.int64()).to_pylist() == [
        expected_time(value)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", TIME_VALUES)
async def test_update_timestamp_values_async(tmp_path, value):
    db = await lancedb.connect_async(tmp_path)
    data = pa.table({"time": pa.array([datetime(2025, 1, 1)], type=pa.timestamp("ns"))})
    table = await db.create_table("times", data)
    await table.update({"time": value})
    assert (await table.to_arrow())["time"].cast(pa.int64()).to_pylist() == [
        expected_time(value)
    ]


@pytest.mark.parametrize("values", [[[1, 2], [3, 4]], []])
def test_nested_array_tuple_matches_list(tmp_path, values):
    db = lancedb.connect(tmp_path)
    data = pa.table(
        {"array": pa.array([[[0, 0]]], type=pa.list_(pa.list_(pa.int64())))}
    )
    control = db.create_table("list_values", data)
    candidate = db.create_table("tuple_values", data)
    tuples = tuple(tuple(value) for value in values)
    try:
        control.update(values={"array": values})
    except ValueError as error:
        reason = (
            "Expected a literal value in array" if values else "concat requires input"
        )
        assert reason in str(error)
        with pytest.raises(ValueError, match=reason):
            candidate.update(values={"array": tuples})
        assert control.to_arrow().equals(data)
        assert candidate.to_arrow().equals(data)
    else:
        candidate.update(values={"array": tuples})
        assert candidate.to_arrow().equals(control.to_arrow())


@pytest.mark.asyncio
@pytest.mark.parametrize("values", [[[1, 2], [3, 4]], []])
async def test_nested_array_tuple_matches_list_async(tmp_path, values):
    db = await lancedb.connect_async(tmp_path)
    data = pa.table(
        {"array": pa.array([[[0, 0]]], type=pa.list_(pa.list_(pa.int64())))}
    )
    control = await db.create_table("list_values", data)
    candidate = await db.create_table("tuple_values", data)
    tuples = tuple(tuple(value) for value in values)
    try:
        await control.update({"array": values})
    except ValueError as error:
        reason = (
            "Expected a literal value in array" if values else "concat requires input"
        )
        assert reason in str(error)
        with pytest.raises(ValueError, match=reason):
            await candidate.update({"array": tuples})
        assert (await control.to_arrow()).equals(data)
        assert (await candidate.to_arrow()).equals(data)
    else:
        await candidate.update({"array": tuples})
        assert (await candidate.to_arrow()).equals(await control.to_arrow())


@pytest.mark.parametrize("unit", ["ps", "fs", "as"])
def test_subnanosecond_update_does_not_round(tmp_path, unit):
    db = lancedb.connect(tmp_path)
    data = pa.table({"time": pa.array([datetime(1970, 1, 1)], type=pa.timestamp("ns"))})
    table = db.create_table("precise", data)
    with pytest.raises(ValueError, match="cannot preserve sub-nanosecond precision"):
        table.update(values={"time": np.datetime64(1, unit)})
    assert table.to_arrow()["time"].cast(pa.int64()).to_pylist() == [0]


@pytest.mark.asyncio
@pytest.mark.parametrize("unit", ["ps", "fs", "as"])
async def test_subnanosecond_update_does_not_round_async(tmp_path, unit):
    db = await lancedb.connect_async(tmp_path)
    data = pa.table({"time": pa.array([datetime(1970, 1, 1)], type=pa.timestamp("ns"))})
    table = await db.create_table("precise", data)
    with pytest.raises(ValueError, match="cannot preserve sub-nanosecond precision"):
        await table.update({"time": np.datetime64(1, unit)})
    assert (await table.to_arrow())["time"].cast(pa.int64()).to_pylist() == [0]


@pytest.mark.parametrize("unit", ["Y", "M", "W", "D"])
def test_update_numpy_date_column(tmp_path, unit):
    value = np.datetime64("2025-01-02", unit)
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "dates", pa.table({"day": pa.array([date(1970, 1, 1)], type=pa.date32())})
    )
    table.update(values={"day": value})
    assert table.to_arrow()["day"].to_pylist() == [value.astype("datetime64[D]").item()]


@pytest.mark.asyncio
@pytest.mark.parametrize("unit", ["Y", "M", "W", "D"])
async def test_update_numpy_date_column_async(tmp_path, unit):
    value = np.datetime64("2025-01-02", unit)
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table(
        "dates", pa.table({"day": pa.array([date(1970, 1, 1)], type=pa.date32())})
    )
    await table.update({"day": value})
    assert (await table.to_arrow())["day"].to_pylist() == [
        value.astype("datetime64[D]").item()
    ]


@pytest.mark.parametrize("timezone", [None, "UTC", "America/New_York", "Asia/Tokyo"])
@pytest.mark.parametrize(
    "value",
    [np.datetime64("2025-01-02T03:04:05.123456789", "ns"), np.datetime64("NaT", "ns")],
)
def test_update_numpy_timestamp_matches_arrow_ingestion(tmp_path, timezone, value):
    dtype = pa.timestamp("ns", tz=timezone)
    ingested = pa.array(np.array([value]), type=dtype)
    db = lancedb.connect(tmp_path)
    table = db.create_table("timestamps", pa.table({"time": ingested}))
    expected = table.to_arrow()["time"].cast(pa.int64()).to_pylist()
    table.update(values={"time": value})
    assert table.to_arrow()["time"].cast(pa.int64()).to_pylist() == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("timezone", [None, "UTC", "America/New_York", "Asia/Tokyo"])
@pytest.mark.parametrize(
    "value",
    [np.datetime64("2025-01-02T03:04:05.123456789", "ns"), np.datetime64("NaT", "ns")],
)
async def test_update_numpy_timestamp_matches_arrow_ingestion_async(
    tmp_path, timezone, value
):
    dtype = pa.timestamp("ns", tz=timezone)
    ingested = pa.array(np.array([value]), type=dtype)
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("timestamps", pa.table({"time": ingested}))
    expected = (await table.to_arrow())["time"].cast(pa.int64()).to_pylist()
    await table.update({"time": value})
    assert (await table.to_arrow())["time"].cast(pa.int64()).to_pylist() == expected
