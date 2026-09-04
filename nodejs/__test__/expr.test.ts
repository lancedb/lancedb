// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
import { col, exprToSQL, func, lit } from "../lancedb/expr";
 
describe("Expr", () => {
  describe("col()", () => {
    it("should produce a quoted column reference", () => {
      expect(col("age").toSQL()).toBe('"age"');
    });
 });
    it("should escape double quotes in column names", () => {
      expect(col('my"col').toSQL()).toBe('"my""col"');
    });
  });
 
  describe("lit()", () => {
    it("should produce a string literal", () => {
      expect(lit("hello").toSQL()).toBe("'hello'");
    });
 
    it("should escape single quotes in strings", () => {
      expect(lit("it's").toSQL()).toBe("'it''s'");
    });
 
    it("should produce a number literal", () => {
      expect(lit(42).toSQL()).toBe("42");
      expect(lit(3.14).toSQL()).toBe("3.14");
    });
 
    it("should produce boolean literals", () => {
      expect(lit(true).toSQL()).toBe("TRUE");
      expect(lit(false).toSQL()).toBe("FALSE");
    });
 
    it("should produce NULL", () => {
      expect(lit(null).toSQL()).toBe("NULL");
    });
 
    it("should produce a date literal", () => {
      const d = new Date("2026-01-09T00:00:00.000Z");
      expect(lit(d).toSQL()).toBe("'2026-01-09T00:00:00.000Z'");
    });
 
    it("should produce an array literal", () => {
      expect(lit([1, 2, 3]).toSQL()).toBe("[1, 2, 3]");
    });
  });
 
  describe("comparisons", () => {
    it("eq", () => {
      expect(col("age").eq(30).toSQL()).toBe('"age" = 30');
    });
 
    it("neq", () => {
      expect(col("age").neq(30).toSQL()).toBe('"age" != 30');
    });
 
    it("lt", () => {
      expect(col("age").lt(30).toSQL()).toBe('"age" < 30');
    });
 
    it("lte", () => {
      expect(col("age").lte(30).toSQL()).toBe('"age" <= 30');
    });
 
    it("gt", () => {
      expect(col("age").gt(30).toSQL()).toBe('"age" > 30');
    });
 
    it("gte", () => {
      expect(col("age").gte(30).toSQL()).toBe('"age" >= 30');
    });
 
    it("should accept Expr on the right-hand side", () => {
      expect(col("a").gt(col("b")).toSQL()).toBe('"a" > "b"');
    });
 
    it("should accept strings as literals (not column refs)", () => {
      expect(col("name").eq("Alice").toSQL()).toBe("\"name\" = 'Alice'");
    });
  });
 
  describe("boolean logic", () => {
    it("and", () => {
      const expr = col("a").gt(1).and(col("b").lt(10));
      expect(expr.toSQL()).toBe('"a" > 1 AND "b" < 10');
    });
 
    it("or", () => {
      const expr = col("a").gt(1).or(col("b").lt(10));
      expect(expr.toSQL()).toBe('"a" > 1 OR "b" < 10');
    });
 
    it("not", () => {
      expect(col("active").not().toSQL()).toBe('NOT "active"');
    });
 
    it("should parenthesize OR inside AND", () => {
      const expr = col("a").gt(1).or(col("b").lt(10)).and(col("c").eq(true));
      // The OR has lower precedence than AND, so should get parens
      expect(expr.toSQL()).toBe('("a" > 1 OR "b" < 10) AND "c" = TRUE');
    });
 
    it("should parenthesize correctly for nested ANDs and ORs", () => {
      // (a > 1 AND b < 10) OR c = true
      const expr = col("a").gt(1).and(col("b").lt(10)).or(col("c").eq(true));
      // AND binds tighter than OR, so no parens needed on the left
      expect(expr.toSQL()).toBe('"a" > 1 AND "b" < 10 OR "c" = TRUE');
    });
  });
 
  describe("generic op()", () => {
    it("addition", () => {
      expect(col("a").op("+", col("b")).toSQL()).toBe('"a" + "b"');
    });
 
    it("subtraction", () => {
      expect(col("a").op("-", 1).toSQL()).toBe('"a" - 1');
    });
 
    it("multiplication", () => {
      expect(col("a").op("*", 2).toSQL()).toBe('"a" * 2');
    });
 
    it("division", () => {
      expect(col("a").op("/", 2).toSQL()).toBe('"a" / 2');
    });
 
    it("modulo", () => {
      expect(col("a").op("%", 2).toSQL()).toBe('"a" % 2');
    });
 
    it("neg", () => {
      expect(col("a").neg().toSQL()).toBe('(-"a")');
    });
 
    it("should respect precedence: + vs *", () => {
      // a + b * c  →  should NOT parenthesize b * c (higher prec)
      const expr = col("a").op("+", col("b").op("*", col("c")));
      expect(expr.toSQL()).toBe('"a" + "b" * "c"');
    });
 
    it("should parenthesize lower-prec child of higher-prec parent", () => {
      // (a + b) * c
      const expr = col("a").op("+", col("b")).op("*", col("c"));
      expect(expr.toSQL()).toBe('("a" + "b") * "c"');
    });
  });
 
  describe("string operations", () => {
    it("lower", () => {
      expect(col("name").lower().toSQL()).toBe('lower("name")');
    });
 
    it("upper", () => {
      expect(col("name").upper().toSQL()).toBe('upper("name")');
    });
 
    it("like", () => {
      expect(col("name").like("%Alice%").toSQL()).toBe(
        "\"name\" LIKE '%Alice%'",
      );
    });
 
    it("contains", () => {
      expect(col("name").contains("Ali").toSQL()).toBe(
        "strpos(\"name\", 'Ali') > 0",
      );
    });
 
    it("startsWith", () => {
      expect(col("name").startsWith("Al").toSQL()).toBe(
        "starts_with(\"name\", 'Al')",
      );
    });
 
    it("endsWith", () => {
      expect(col("name").endsWith("ce").toSQL()).toBe(
        "ends_with(\"name\", 'ce')",
      );
    });
 
    it("length", () => {
      expect(col("name").length().toSQL()).toBe('length("name")');
    });
 
    it("trim", () => {
      expect(col("name").trim().toSQL()).toBe('trim("name")');
    });
  });
 
  describe("null checks", () => {
    it("isNull", () => {
      expect(col("x").isNull().toSQL()).toBe('"x" IS NULL');
    });
 
    it("isNotNull", () => {
      expect(col("x").isNotNull().toSQL()).toBe('"x" IS NOT NULL');
    });
  });
 
  describe("IN / BETWEEN", () => {
    it("isIn", () => {
      expect(col("status").isIn(["active", "pending"]).toSQL()).toBe(
        "\"status\" IN ('active', 'pending')",
      );
    });
 
    it("isNotIn", () => {
      expect(col("status").isNotIn([1, 2, 3]).toSQL()).toBe(
        '"status" NOT IN (1, 2, 3)',
      );
    });
 
    it("between", () => {
      expect(col("age").between(18, 65).toSQL()).toBe(
        '"age" BETWEEN 18 AND 65',
      );
    });
 
    it("notBetween", () => {
      expect(col("age").notBetween(18, 65).toSQL()).toBe(
        '"age" NOT BETWEEN 18 AND 65',
      );
    });
  });
 
  describe("cast", () => {
    it("should produce CAST expression", () => {
      expect(col("id").cast("VARCHAR").toSQL()).toBe('CAST("id" AS VARCHAR)');
    });
  });
 
  describe("alias", () => {
    it("should produce AS clause", () => {
      expect(col("a").op("+", col("b")).alias("sum_ab").toSQL()).toBe(
        '"a" + "b" AS "sum_ab"',
      );
    });
  });
 
  describe("func()", () => {
    it("should call a function with no args", () => {
      expect(func("now").toSQL()).toBe("now()");
    });
 
    it("should call a function with column and literal args", () => {
      expect(func("coalesce", col("a"), lit(0)).toSQL()).toBe(
        'coalesce("a", 0)',
      );
    });
 
    it("should auto-wrap raw values", () => {
      expect(func("substr", col("name"), 1, 3).toSQL()).toBe(
        'substr("name", 1, 3)',
      );
    });
  });
 
  describe("complex expressions", () => {
    it("should handle a realistic filter", () => {
      const expr = col("created_at")
        .gt(new Date("2026-01-09T00:00:00.000Z"))
        .and(col("id").eq(42));
      expect(expr.toSQL()).toBe(
        '"created_at" > \'2026-01-09T00:00:00.000Z\' AND "id" = 42',
      );
    });
 
    it("should handle a realistic projection expression", () => {
      const expr = col("price").op("*", col("quantity")).alias("total");
      expect(expr.toSQL()).toBe('"price" * "quantity" AS "total"');
    });
 
    it("should handle deeply nested expressions", () => {
      // (a + b) * (c - d) > 0 AND name IS NOT NULL
      const expr = col("a")
        .op("+", col("b"))
        .op("*", col("c").op("-", col("d")))
        .gt(0)
        .and(col("name").isNotNull());
      expect(expr.toSQL()).toBe(
        '("a" + "b") * ("c" - "d") > 0 AND "name" IS NOT NULL',
      );
    });
 
    it("should handle composing via variables", () => {
      const isAdult = col("age").gte(18);
      const isActive = col("status").eq("active");
      const isVerified = col("verified").eq(true);
      const filter = isAdult.and(isActive.or(isVerified));
      expect(filter.toSQL()).toBe(
        '"age" >= 18 AND ("status" = \'active\' OR "verified" = TRUE)',
      );
    });
  });
 
  describe("exprToSQL helper", () => {
    it("should pass through raw strings", () => {
      expect(exprToSQL("age > 30")).toBe("age > 30");
    });
 
    it("should serialize Expr objects", () => {
      expect(exprToSQL(col("age").gt(30))).toBe('"age" > 30');
    });
  });
 
 