# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

import lancedb
from lancedb.read_files import FileSource, _detect_format, read_files


# -- helpers ------------------------------------------------------------------


def make_table() -> pa.Table:
    return pa.table({"id": pa.array([1, 2, 3]), "name": pa.array(["a", "b", "c"])})


def write_parquet(path, table: pa.Table) -> None:
    pq.write_table(table, path)


def write_csv(path) -> None:
    path.write_text("id,name\n1,a\n2,b\n3,c\n")


# -- _detect_format -----------------------------------------------------------


def test_detect_format_single_file():
    assert _detect_format("file.parquet") == "parquet"
    assert _detect_format("file.csv") == "csv"
    assert _detect_format("dataset.lance") == "lance"


def test_detect_format_glob():
    assert _detect_format("data/*.parquet") == "parquet"
    assert _detect_format("data/*.csv") == "csv"


def test_detect_format_unknown_raises():
    with pytest.raises(ValueError, match="Unsupported file format"):
        _detect_format("file.json")


# -- FileSource / read_files --------------------------------------------------


def test_read_single_parquet(tmp_path):
    write_parquet(tmp_path / "test.parquet", make_table())
    source = read_files(str(tmp_path / "test.parquet"))
    assert isinstance(source, FileSource)
    assert len(source.schema) == 2
    assert source.schema.field("id").type == pa.int64()


def test_read_single_csv(tmp_path):
    write_csv(tmp_path / "test.csv")
    source = read_files(str(tmp_path / "test.csv"))
    assert isinstance(source, FileSource)
    assert len(source.schema) == 2


def test_read_parquet_glob(tmp_path):
    tbl = make_table()
    write_parquet(tmp_path / "part1.parquet", tbl)
    write_parquet(tmp_path / "part2.parquet", tbl)
    source = read_files(str(tmp_path / "*.parquet"))
    assert isinstance(source, FileSource)
    assert len(source.schema) == 2


def test_read_lance(tmp_path):
    lance = pytest.importorskip("lance")
    uri = str(tmp_path / "test.lance")
    tbl = make_table()
    lance.write_dataset(tbl, uri)
    source = read_files(uri)
    assert isinstance(source, FileSource)
    assert len(source.schema) == 2


def test_lance_glob_raises():
    with pytest.raises(ValueError, match="Glob patterns are not supported"):
        read_files("./data/*.lance")


def test_unknown_format_raises():
    with pytest.raises(ValueError, match="Unsupported file format"):
        read_files("data.json")


def test_glob_no_matches_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="No files matched"):
        read_files(str(tmp_path / "*.parquet"))


# -- Integration: table.add ---------------------------------------------------


def test_add_from_read_files_parquet(tmp_path):
    write_parquet(tmp_path / "test.parquet", make_table())
    db = lancedb.connect(tmp_path / "db")
    source = read_files(str(tmp_path / "test.parquet"))
    tbl = db.create_table("test", source)
    assert tbl.count_rows() == 3


def test_add_from_read_files_parquet_glob(tmp_path):
    tbl = make_table()
    write_parquet(tmp_path / "part1.parquet", tbl)
    write_parquet(tmp_path / "part2.parquet", tbl)
    db = lancedb.connect(tmp_path / "db")
    source = read_files(str(tmp_path / "*.parquet"))
    result = db.create_table("test", source)
    assert result.count_rows() == 6  # 3 rows × 2 files


def test_add_from_read_files_csv(tmp_path):
    write_csv(tmp_path / "test.csv")
    db = lancedb.connect(tmp_path / "db")
    source = read_files(str(tmp_path / "test.csv"))
    tbl = db.create_table("test", source)
    assert tbl.count_rows() == 3


def test_add_from_read_files_lance(tmp_path):
    lance = pytest.importorskip("lance")
    uri = str(tmp_path / "test.lance")
    lance.write_dataset(make_table(), uri)
    db = lancedb.connect(tmp_path / "db")
    source = read_files(uri)
    tbl = db.create_table("test", source)
    assert tbl.count_rows() == 3


def test_is_rescannable(tmp_path):
    """FileSource can be scanned twice (e.g. for retry or schema check)."""
    from lancedb.scannable import to_scannable

    write_parquet(tmp_path / "test.parquet", make_table())
    source = read_files(str(tmp_path / "test.parquet"))
    scannable = to_scannable(source)
    assert scannable.rescannable is True

    batches1 = scannable.reader().read_all()
    batches2 = scannable.reader().read_all()
    assert batches1.num_rows == batches2.num_rows == 3
