# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Tests for the type-safe expression builder API."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pytest

import lancedb
from lancedb.expr import Expr, col, func, lit


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
        assert e.to_sql() == "lower(name)"

    def test_func_unknown_raises(self):
        with pytest.raises(Exception):
            func("not_a_real_function", col("x"))

    def test_lit_date(self):
        e = lit(date(2024, 1, 1))
        assert isinstance(e, Expr)

    def test_lit_datetime(self):
        # Naive datetime
        e = lit(datetime(2024, 1, 1, 10, 0))
        assert isinstance(e, Expr)

    def test_lit_datetime_tz(self):
        # Timezone-aware datetime
        tz = timezone(timedelta(hours=5))
        dt = datetime(2024, 1, 1, 10, 0, tzinfo=tz)
        e = lit(dt)
        assert isinstance(e, Expr)

    def test_lit_decimal_precision(self):
        # High precision Decimal that would be rounded if converted to float
        d = Decimal("1.234567890123456789")
        e = lit(d)
        assert isinstance(e, Expr)


class TestExprOperators:
    def test_eq_operator(self):
        e = col("x") == lit(1)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x = 1)"

    def test_ne_operator(self):
        e = col("x") != lit(1)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x <> 1)"

    def test_lt_operator(self):
        e = col("age") < lit(18)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(age < 18)"

    def test_le_operator(self):
        e = col("age") <= lit(18)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(age <= 18)"

    def test_gt_operator(self):
        e = col("age") > lit(18)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(age > 18)"

    def test_ge_operator(self):
        e = col("age") >= lit(18)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(age >= 18)"

    def test_and_operator(self):
        e = (col("age") > lit(18)) & (col("status") == lit("active"))
        assert isinstance(e, Expr)
        assert e.to_sql() == "((age > 18) AND (status = 'active'))"

    def test_or_operator(self):
        e = (col("a") == lit(1)) | (col("b") == lit(2))
        assert isinstance(e, Expr)
        assert e.to_sql() == "((a = 1) OR (b = 2))"

    def test_invert_operator(self):
        e = ~(col("active") == lit(True))
        assert isinstance(e, Expr)
        assert e.to_sql() == "NOT (active = true)"

    def test_add_operator(self):
        e = col("x") + lit(1)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x + 1)"

    def test_sub_operator(self):
        e = col("x") - lit(1)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x - 1)"

    def test_mul_operator(self):
        e = col("price") * lit(1.1)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(price * 1.1)"

    def test_div_operator(self):
        e = col("total") / lit(2)
        assert isinstance(e, Expr)
        assert e.to_sql() == "(total / 2)"

    def test_radd(self):
        e = lit(1) + col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(1 + x)"

    def test_rmul(self):
        e = lit(2) * col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(2 * x)"

    def test_coerce_plain_int(self):
        # Operators should auto-wrap plain Python values via lit()
        e = col("age") > 18
        assert isinstance(e, Expr)
        assert e.to_sql() == "(age > 18)"

    def test_coerce_plain_str(self):
        e = col("name") == "alice"
        assert isinstance(e, Expr)
        assert e.to_sql() == "(name = 'alice')"

    def test_reflexive_comparisons(self):
        # 10 < col("age") swaps to col("age") > 10
        assert (10 < col("age")).to_sql() == "(age > 10)"
        assert (10 <= col("age")).to_sql() == "(age >= 10)"
        assert (10 > col("age")).to_sql() == "(age < 10)"
        assert (10 >= col("age")).to_sql() == "(age <= 10)"
        assert (10 == col("age")).to_sql() == "(age = 10)"
        assert (10 != col("age")).to_sql() == "(age <> 10)"

    def test_reflexive_logical(self):
        # True & Expr calls Expr.__rand__(True)
        assert (True & (col("age") > 18)).to_sql() == "(true AND (age > 18))"
        assert (False | (col("age") > 18)).to_sql() == "(false OR (age > 18))"


class TestExprStringMethods:
    def test_lower(self):
        e = col("name").lower()
        assert isinstance(e, Expr)
        assert e.to_sql() == "lower(name)"

    def test_upper(self):
        e = col("name").upper()
        assert isinstance(e, Expr)
        assert e.to_sql() == "upper(name)"

    def test_contains(self):
        e = col("text").contains(lit("hello"))
        assert isinstance(e, Expr)
        assert e.to_sql() == "contains(text, 'hello')"

    def test_contains_with_str_coerce(self):
        e = col("text").contains("hello")
        assert isinstance(e, Expr)
        assert e.to_sql() == "contains(text, 'hello')"

    def test_chained_lower_eq(self):
        e = col("name").lower() == lit("alice")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(lower(name) = 'alice')"


class TestExprCast:
    def test_cast_string(self):
        e = col("id").cast("string")
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(id AS VARCHAR)"

    def test_cast_int32(self):
        e = col("score").cast("int32")
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(score AS INTEGER)"

    def test_cast_float64(self):
        e = col("val").cast("float64")
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(val AS DOUBLE)"

    def test_cast_pyarrow_type(self):
        e = col("score").cast(pa.int32())
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(score AS INTEGER)"

    def test_cast_pyarrow_float64(self):
        e = col("val").cast(pa.float64())
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(val AS DOUBLE)"

    def test_cast_pyarrow_string(self):
        e = col("id").cast(pa.string())
        assert isinstance(e, Expr)
        assert e.to_sql() == "CAST(id AS VARCHAR)"

    def test_cast_pyarrow_and_string_equivalent(self):
        # pa.int32() and "int32" should produce equivalent SQL
        sql_str = col("x").cast("int32").to_sql()
        sql_pa = col("x").cast(pa.int32()).to_sql()
        assert sql_str == sql_pa


class TestExprNamedMethods:
    def test_eq_method(self):
        e = col("x").eq(lit(1))
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x = 1)"

    def test_gt_method(self):
        e = col("x").gt(lit(0))
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x > 0)"

    def test_and_method(self):
        e = col("x").gt(lit(0)).and_(col("y").lt(lit(10)))
        assert isinstance(e, Expr)
        assert e.to_sql() == "((x > 0) AND (y < 10))"

    def test_or_method(self):
        e = col("x").eq(lit(1)).or_(col("x").eq(lit(2)))
        assert isinstance(e, Expr)
        assert e.to_sql() == "((x = 1) OR (x = 2))"


class TestExprRepr:
    def test_repr(self):
        e = col("age") > lit(18)
        assert repr(e) == "Expr((age > 18))"

    def test_to_sql(self):
        e = col("age") > 18
        assert e.to_sql() == "(age > 18)"

    def test_unhashable(self):
        e = col("x")
        with pytest.raises(TypeError):
            {e: 1}


class TestExprReflexive:
    def test_reflexive_eq(self):
        e = 1 == col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x = 1)"

    def test_reflexive_ne(self):
        e = 1 != col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x <> 1)"

    def test_reflexive_lt(self):
        # 1 < x  =>  (x > 1)
        e = 1 < col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x > 1)"

    def test_reflexive_gt(self):
        # 1 > x  =>  (x < 1)
        e = 1 > col("x")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(x < 1)"

    def test_reflexive_and(self):
        e = True & col("active")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(true AND active)"

    def test_reflexive_or(self):
        e = False | col("inactive")
        assert isinstance(e, Expr)
        assert e.to_sql() == "(false OR inactive)"


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
        result = simple_table.search().where(col("name") == lit("Bob")).to_arrow()
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


# ── column name edge cases ────────────────────────────────────────────────────


class TestColNaming:
    """Unit tests verifying that col() preserves identifiers exactly.

    Identifiers that need quoting (camelCase, spaces, leading digits, unicode)
    are wrapped in backticks to match the lance SQL parser's dialect.
    """

    def test_camel_case_preserved_in_sql(self):
        # camelCase is quoted with backticks so the case round-trips correctly.
        assert col("firstName").to_sql() == "`firstName`"

    def test_camel_case_in_expression(self):
        assert (col("firstName") > lit(18)).to_sql() == "(`firstName` > 18)"

    def test_space_in_name_quoted(self):
        assert col("first name").to_sql() == "`first name`"

    def test_space_in_expression(self):
        assert (col("first name") == lit("A")).to_sql() == "(`first name` = 'A')"

    def test_leading_digit_quoted(self):
        assert col("2fast").to_sql() == "`2fast`"

    def test_unicode_quoted(self):
        assert col("名前").to_sql() == "`名前`"

    def test_snake_case_unquoted(self):
        # Plain snake_case needs no quoting.
        assert col("first_name").to_sql() == "first_name"


@pytest.fixture
def special_col_table(tmp_path):
    db = lancedb.connect(str(tmp_path))
    data = pa.table(
        {
            "firstName": ["Alice", "Bob", "Charlie"],
            "first name": ["A", "B", "C"],
            "score": [10, 20, 30],
        }
    )
    return db.create_table("special", data)


class TestColNamingIntegration:
    def test_camel_case_filter(self, special_col_table):
        result = (
            special_col_table.search()
            .where(col("firstName") == lit("Alice"))
            .to_arrow()
        )
        assert result.num_rows == 1
        assert result["firstName"][0].as_py() == "Alice"

    def test_space_in_col_filter(self, special_col_table):
        result = (
            special_col_table.search().where(col("first name") == lit("B")).to_arrow()
        )
        assert result.num_rows == 1

    def test_camel_case_projection(self, special_col_table):
        result = (
            special_col_table.search()
            .select({"upper_name": col("firstName").upper()})
            .to_arrow()
        )
        assert "upper_name" in result.schema.names
        assert sorted(result["upper_name"].to_pylist()) == ["ALICE", "BOB", "CHARLIE"]


@pytest.fixture
def type_check_table(tmp_path):
    """Fixture that creates a table with Date32 and Decimal128 columns."""
    db = lancedb.connect(str(tmp_path))
    schema = pa.schema(
        [
            ("date", pa.date32()),
            ("decimal", pa.decimal128(10, 2)),
            ("binary", pa.binary()),
        ]
    )
    data = pa.table(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "decimal": [Decimal("10.50"), Decimal("20.75")],
            "binary": [b"\x01", b"\x02"],
        },
        schema=schema,
    )
    return db.create_table("extended_types", data)


class TestExtendedTypeIntegration:
    """Integration tests verifying that typed literals work correctly in filters."""

    def test_date_integration(self, type_check_table):
        """Verify that Date32 literals are correctly parsed and filtered."""
        result = (
            type_check_table.search()
            .where(col("date") == lit(date(2024, 1, 1)))
            .to_arrow()
        )
        assert result.num_rows == 1
        assert result["date"][0].as_py() == date(2024, 1, 1)

    def test_decimal_integration(self, type_check_table):
        """Verify that high-precision Decimal literals avoid float-rounding issues."""
        val1 = Decimal("1.50")
        val2 = Decimal("2.50")

        db = lancedb.connect(
            str(type_check_table.uri).replace("extended_types", "precision_test")
        )
        schema = pa.schema([("val", pa.decimal128(4, 2))])
        table = db.create_table(
            "precision_test",
            pa.table({"val": [val1, val2]}, schema=schema),
            mode="overwrite",
        )

        # This will only work if lit(val2) is a true Decimal128(38, 18)
        # or if DataFusion can cast it from Decimal128(19, 18)
        result = table.search().where(col("val") < lit(val2)).to_arrow()
        assert result.num_rows == 1
        assert result["val"][0].as_py() == val1

    def test_binary_integration(self, type_check_table):
        """Verify that Binary literals are correctly filtered."""
        result = (
            type_check_table.search().where(col("binary") == lit(b"\x01")).to_arrow()
        )
        assert result.num_rows == 1
        assert result["binary"][0].as_py() == b"\x01"
