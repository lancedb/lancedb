// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/**
 * SQL expression helpers for type-safe query construction.
 *
 * These utilities make it easy to reference column names — including names
 * with camelCase, spaces, or other special characters — without writing raw
 * SQL strings by hand.
 *
 * @example
 * ```ts
 * import { col, lit } from "@lancedb/lancedb";
 *
 * // camelCase column (automatically quoted with backticks)
 * await table.query().where(`${col("firstName")} = ${lit("Alice")}`).toArray();
 *
 * // plain column (no quoting needed)
 * await table.query().where(`${col("age")} > ${lit(18)}`).toArray();
 * ```
 */

import { type IntoSql, toSQL } from "./util";

// SQL reserved words that need quoting even though they match [a-z_][a-z0-9_]*
const SQL_RESERVED_WORDS = new Set([
  "all",
  "and",
  "as",
  "asc",
  "between",
  "by",
  "case",
  "create",
  "delete",
  "desc",
  "distinct",
  "drop",
  "else",
  "end",
  "exists",
  "false",
  "from",
  "group",
  "having",
  "in",
  "index",
  "insert",
  "is",
  "join",
  "like",
  "limit",
  "not",
  "null",
  "offset",
  "on",
  "or",
  "order",
  "select",
  "table",
  "then",
  "true",
  "union",
  "update",
  "when",
  "where",
]);

/**
 * Return true when the identifier must be wrapped in backticks.
 *
 * An identifier needs quoting when it:
 * - starts with a digit
 * - contains characters outside `[a-zA-Z0-9_]`
 * - contains upper-case letters (Lance SQL normalises unquoted identifiers to
 *   lower-case, which would break case-sensitive schemas like camelCase)
 * - is a SQL reserved word
 */
function needsQuoting(name: string): boolean {
  if (name.length === 0) return true;
  if (/^\d/.test(name)) return true;
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) return true;
  if (/[A-Z]/.test(name)) return true;
  if (SQL_RESERVED_WORDS.has(name.toLowerCase())) return true;
  return false;
}

/**
 * Reference a table column by name, quoting it with backticks if necessary.
 *
 * Column names that contain upper-case letters (e.g. camelCase), spaces,
 * or other special characters must be quoted in Lance SQL.  This function
 * handles that automatically so you don't have to remember which names need
 * backtick-quoting.
 *
 * @param name - The column name to reference.
 * @returns A SQL identifier string, backtick-quoted when necessary.
 *
 * @example
 * ```ts
 * // camelCase → quoted automatically
 * col("firstName")   // "`firstName`"
 *
 * // plain lowercase → no quoting needed
 * col("age")         // "age"
 *
 * // column with spaces → quoted
 * col("first name")  // "`first name`"
 * ```
 *
 * Use `col()` inside template literals when building `.where()` predicates:
 *
 * ```ts
 * await table.query()
 *   .where(`${col("firstName")} = ${lit("Alice")} AND ${col("age")} > ${lit(18)}`)
 *   .toArray();
 * ```
 */
export function col(name: string): string {
  if (needsQuoting(name)) {
    // Escape any backticks already present in the name, then wrap.
    return "`" + name.replace(/`/g, "``") + "`";
  }
  return name;
}

/**
 * Convert a JavaScript value to a SQL literal string.
 *
 * Supported types: `string`, `number`, `boolean`, `null`, `Date`,
 * `ArrayBuffer`, `Buffer`, and arrays of the above.
 *
 * @param value - The value to convert.
 * @returns A SQL literal string safe to embed in a `.where()` predicate.
 *
 * @example
 * ```ts
 * lit("Alice")   // "'Alice'"
 * lit(42)        // "42"
 * lit(true)      // "TRUE"
 * lit(null)      // "NULL"
 * ```
 */
export function lit(value: IntoSql): string {
  return toSQL(value);
}
