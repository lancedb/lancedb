// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as tmp from "tmp";

import { type Table, connect } from "../lancedb";
import { Expr, col, func, lit, litBool, litInt } from "../lancedb/expr";

describe("expr", () => {
  describe("col", () => {
    it("returns an Expr instance", () => {
      const e = col("age");
      expect(e).toBeInstanceOf(Expr);
    });

    it("preserves camelCase in toSql()", () => {
      expect(col("firstName").toSql()).toBe("`firstName`");
    });

    it("preserves snake_case without quotes", () => {
      expect(col("first_name").toSql()).toBe("first_name");
    });

    it("Quotes identifiers with spaces", () => {
      expect(col("first name").toSql()).toBe("`first name`");
    });

    it("Quotes identifiers with leading digits", () => {
      expect(col("2fast").toSql()).toBe("`2fast`");
    });

    it("Quotes unicode identifiers", () => {
      expect(col("名前").toSql()).toBe("`名前`");
    });
  });

  describe("lit", () => {
    it("creates a string literal", () => {
      expect(lit("hello").toSql()).toBe("'hello'");
    });

    it("creates an integer literal", () => {
      expect(lit(42).toSql()).toBe("42");
    });

    it("creates a float literal", () => {
      expect(lit(3.14).toSql()).toBe("3.14");
    });
  });

  describe("numeric literals", () => {
    it.each([Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER])(
      "preserves the safe integer boundary %s",
      (value) => {
        expect(lit(value).toSql()).toBe(String(value));
        expect(litInt(value).toSql()).toBe(String(value));
      },
    );

    it.each([1e20, -1e20])("preserves the large number %s", (value) => {
      expect(Number(lit(value).toSql())).toBe(value);
      expect(col("price").gt(value).toSql()).toBe(
        col("price").gt(lit(value)).toSql(),
      );
    });

    it.each([1.5, 1e20, -1e20, Number.NaN, Number.POSITIVE_INFINITY])(
      "rejects %s as an explicit integer literal",
      (value) => expect(() => litInt(value)).toThrow(RangeError),
    );
  });

  describe("Expr operators", () => {
    it("eq", () => {
      expect(col("x").eq(lit(1)).toSql()).toBe("(x = 1)");
    });

    it("ne", () => {
      expect(col("x").ne(lit(1)).toSql()).toBe("(x <> 1)");
    });

    it("lt", () => {
      expect(col("age").lt(lit(18)).toSql()).toBe("(age < 18)");
    });

    it("lte", () => {
      expect(col("age").lte(lit(18)).toSql()).toBe("(age <= 18)");
    });

    it("gt", () => {
      expect(col("age").gt(lit(18)).toSql()).toBe("(age > 18)");
    });

    it("gte", () => {
      expect(col("age").gte(lit(18)).toSql()).toBe("(age >= 18)");
    });

    it("and", () => {
      const e = col("age")
        .gt(lit(18))
        .and(col("status").eq(lit("active")));
      expect(e.toSql()).toBe("((age > 18) AND (`status` = 'active'))");
    });

    it("or", () => {
      const e = col("a")
        .eq(lit(1))
        .or(col("b").eq(lit(2)));
      expect(e.toSql()).toBe("((a = 1) OR (b = 2))");
    });

    it("not", () => {
      const e = col("active").eq(litBool(true)).not();
      expect(e.toSql()).toBe("NOT (active = true)");
    });

    it("add", () => {
      expect(col("x").add(lit(1)).toSql()).toBe("(x + 1)");
    });

    it("sub", () => {
      expect(col("x").sub(lit(1)).toSql()).toBe("(x - 1)");
    });

    it("mul", () => {
      expect(col("price").mul(lit(1.1)).toSql()).toBe("(price * 1.1)");
    });

    it("div", () => {
      expect(col("total").div(lit(2)).toSql()).toBe("(total / 2)");
    });
  });

  describe("Expr string methods", () => {
    it("lower", () => {
      expect(col("name").lower().toSql()).toBe("lower(`name`)");
    });

    it("upper", () => {
      expect(col("name").upper().toSql()).toBe("upper(`name`)");
    });

    it("contains", () => {
      expect(col("text").contains(lit("hello")).toSql()).toBe(
        "contains(`text`, 'hello')",
      );
    });
  });

  describe("membership and casts", () => {
    it("accepts scalar list members", () => {
      expect(col("age").isIn([18, 21]).toSql()).toBe("age IN (18, 21)");
    });
    it("casts to an Arrow type", () => {
      expect(col("age").cast("Int64").toSql()).toBe("arrow_cast(age, 'Int64')");
    });
    it("rejects an invalid Arrow type", () => {
      expect(() => col("age").cast("invalid")).toThrow("Invalid data type");
    });
  });

  describe("func", () => {
    it("lower", () => {
      expect(func("lower", col("name")).toSql()).toBe("lower(`name`)");
    });
  });

  describe("camelCase integration", () => {
    let db: Awaited<ReturnType<typeof connect>>;
    let table: Table;
    let tmpDir: tmp.DirResult;

    beforeAll(async () => {
      tmpDir = tmp.dirSync({ unsafeCleanup: true });
      db = await connect(tmpDir.name);
      table = await db.createTable("test", [
        { id: 1, firstName: "Alice", lastName: "Smith" },
        { id: 2, firstName: "Bob", lastName: "Jones" },
        { id: 3, firstName: "Charlie", lastName: "Brown" },
      ]);
    });

    afterAll(() => {
      table.close();
      db.close();
      tmpDir.removeCallback();
    });

    it("filters on camelCase column", async () => {
      const results = await table
        .query()
        .where(col("firstName").eq(lit("Alice")))
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].firstName).toBe("Alice");
    });

    it("filters on multiple camelCase columns", async () => {
      const results = await table
        .query()
        .where(
          col("firstName")
            .eq(lit("Bob"))
            .and(col("lastName").eq(lit("Jones"))),
        )
        .toArray();
      expect(results.length).toBe(1);
      expect(results[0].firstName).toBe("Bob");
    });

    it("preserves an unsafe integer filter threshold", async () => {
      const prices = await db.createTable("prices", [
        { price: 9.5e18 },
        { price: 1.1e20 },
      ]);
      try {
        const results = await prices
          .query()
          .where(col("price").gt(1e20))
          .toArray();
        expect(results.map((row) => row.price)).toEqual([1.1e20]);
      } finally {
        prices.close();
      }
    });

    it("filters with gt on camelCase column", async () => {
      const results = await table
        .query()
        .where(col("id").gt(lit(1)))
        .toArray();
      expect(results.length).toBe(2);
    });
  });
});
