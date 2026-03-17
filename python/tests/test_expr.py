# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for the type-safe expression builder API."""

import pytest
import pyarrow as pa
import lancedb
from lancedb.expr import Expr, col, lit, func


# ── unit tests for Expr construction ─────────────────────────────────────────


class TestExprConstruction:
    def test_col_returns_expr(self):
        e = col("age")
        assert isinstance(e, Expr)

    def test_lit_int(self):
        e = lit(42)
        assert isinstance(e, Expr)

    def test_lit_float(self):
        e = lit(3.14)
        assert isinstance(e, Expr)

    def test_lit_str(self):
        e = lit("hello")
        assert isinstance(e, Expr)

    def test_lit_bool(self):
        e = lit(True)
        assert isinstance(e, Expr)

    def test_lit_unsupported_type_raises(self):
        with pytest.raises(Exception):
            lit([1, 2, 3])

    def test_func(self):
        e = func("lower", col("name"))
        assert isinstance(e, Expr)
        assert "lower" in e.to_sql().lower()

    def test_func_unknown_raises(self):
        with pytest.raises(Exception):
            func("not_a_real_function", col("x"))


class TestExprOperators:
    def test_eq_operator(self):
        e = col("x") == lit(1)
        assert isinstance(e, Expr)
        sql = e.to_sql()
        assert "x" in sql and "1" in sql

    def test_ne_operator(self):
        e = col("x") != lit(1)
        assert isinstance(e, Expr)

    def test_lt_operator(self):
        e = col("age") < lit(18)
        assert isinstance(e, Expr)

    def test_le_operator(self):
        e = col("age") <= lit(18)
        assert isinstance(e, Expr)

    def test_gt_operator(self):
        e = col("age") > lit(18)
        assert isinstance(e, Expr)

    def test_ge_operator(self):
        e = col("age") >= lit(18)
        assert isinstance(e, Expr)

    def test_and_operator(self):
        e = (col("age") > lit(18)) & (col("status") == lit("active"))
        assert isinstance(e, Expr)
        sql = e.to_sql()
        assert "age" in sql and "status" in sql

    def test_or_operator(self):
        e = (col("a") == lit(1)) | (col("b") == lit(2))
        assert isinstance(e, Expr)

    def test_invert_operator(self):
        e = ~(col("active") == lit(True))
        assert isinstance(e, Expr)

    def test_add_operator(self):
        e = col("x") + lit(1)
        assert isinstance(e, Expr)

    def test_sub_operator(self):
        e = col("x") - lit(1)
        assert isinstance(e, Expr)

    def test_mul_operator(self):
        e = col("price") * lit(1.1)
        assert isinstance(e, Expr)
        sql = e.to_sql()
        assert "price" in sql

    def test_div_operator(self):
        e = col("total") / lit(2)
        assert isinstance(e, Expr)

    def test_radd(self):
        e = lit(1) + col("x")
        assert isinstance(e, Expr)

    def test_rmul(self):
        e = lit(2) * col("x")
        assert isinstance(e, Expr)

    def test_coerce_plain_int(self):
        # Operators should auto-wrap plain Python values via lit()
        e = col("age") > 18
        assert isinstance(e, Expr)

    def test_coerce_plain_str(self):
        e = col("name") == "alice"
        assert isinstance(e, Expr)


class TestExprStringMethods:
    def test_lower(self):
        e = col("name").lower()
        assert isinstance(e, Expr)
        assert "lower" in e.to_sql().lower()

    def test_upper(self):
        e = col("name").upper()
        assert isinstance(e, Expr)
        assert "upper" in e.to_sql().lower()

    def test_contains(self):
        e = col("text").contains(lit("hello"))
        assert isinstance(e, Expr)

    def test_contains_with_str_coerce(self):
        e = col("text").contains("hello")
        assert isinstance(e, Expr)

    def test_chained_lower_eq(self):
        e = col("name").lower() == lit("alice")
        assert isinstance(e, Expr)


class TestExprCast:
    def test_cast_string(self):
        e = col("id").cast("string")
        assert isinstance(e, Expr)

    def test_cast_int32(self):
        e = col("score").cast("int32")
        assert isinstance(e, Expr)

    def test_cast_float64(self):
        e = col("val").cast("float64")
        assert isinstance(e, Expr)

    def test_cast_unknown_raises(self):
        with pytest.raises(Exception):
            col("x").cast("not_a_type")


class TestExprNamedMethods:
    def test_eq_method(self):
        e = col("x").eq(lit(1))
        assert isinstance(e, Expr)

    def test_gt_method(self):
        e = col("x").gt(lit(0))
        assert isinstance(e, Expr)

    def test_and_method(self):
        e = col("x").gt(lit(0)).and_(col("y").lt(lit(10)))
        assert isinstance(e, Expr)

    def test_or_method(self):
        e = col("x").eq(lit(1)).or_(col("x").eq(lit(2)))
        assert isinstance(e, Expr)


class TestExprRepr:
    def test_repr(self):
        e = col("age") > lit(18)
        r = repr(e)
        assert r.startswith("Expr(")

    def test_to_sql(self):
        e = col("age") > lit(18)
        sql = e.to_sql()
        assert "age" in sql
        assert "18" in sql

    def test_unhashable(self):
        e = col("x")
        with pytest.raises(TypeError):
            {e: 1}


# ── integration tests: end-to-end query against a real table ─────────────────


@pytest.fixture
def simple_table(tmp_path):
    db = lancedb.connect(str(tmp_path))
    data = pa.table(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "alice", "BOB"],
            "age": [25, 17, 30, 22, 15],
            "score": [1.5, 2.0, 3.5, 4.0, 0.5],
        }
    )
    return db.create_table("test", data)


class TestExprFilter:
    def test_simple_gt_filter(self, simple_table):
        result = simple_table.search().where(col("age") > lit(20)).to_arrow()
        assert result.num_rows == 3  # ages 25, 30, 22

    def test_compound_and_filter(self, simple_table):
        result = (
            simple_table.search()
            .where((col("age") > lit(18)) & (col("score") > lit(2.0)))
            .to_arrow()
        )
        assert result.num_rows == 2  # (30, 3.5) and (22, 4.0)

    def test_string_equality_filter(self, simple_table):
        result = (
            simple_table.search().where(col("name") == lit("Bob")).to_arrow()
        )
        assert result.num_rows == 1

    def test_or_filter(self, simple_table):
        result = (
            simple_table.search()
            .where((col("age") < lit(18)) | (col("age") > lit(28)))
            .to_arrow()
        )
        assert result.num_rows == 3  # ages 17, 30, 15

    def test_coercion_no_lit(self, simple_table):
        # Python values should be auto-coerced
        result = simple_table.search().where(col("age") > 20).to_arrow()
        assert result.num_rows == 3

    def test_string_sql_still_works(self, simple_table):
        # Backwards compatibility: plain strings still accepted
        result = simple_table.search().where("age > 20").to_arrow()
        assert result.num_rows == 3


class TestExprProjection:
    def test_select_with_expr(self, simple_table):
        result = (
            simple_table.search()
            .select({"double_score": col("score") * lit(2)})
            .to_arrow()
        )
        assert "double_score" in result.schema.names

    def test_select_mixed_str_and_expr(self, simple_table):
        result = (
            simple_table.search()
            .select({"id": "id", "double_score": col("score") * lit(2)})
            .to_arrow()
        )
        assert "id" in result.schema.names
        assert "double_score" in result.schema.names

    def test_select_list_of_columns(self, simple_table):
        # Plain list of str still works
        result = simple_table.search().select(["id", "name"]).to_arrow()
        assert result.schema.names == ["id", "name"]
