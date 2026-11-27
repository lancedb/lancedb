// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import {
  Expr as NativeExpr,
  col as nativeCol,
  func as nativeFunc,
  lit as nativeLit,
  litBool as nativeLitBool,
  litInt as nativeLitInt,
} from "./native.js";

export { Expr as NativeExpr } from "./native";

/** Values accepted by expression operators. */
export type ExprLike = Expr | string | number | boolean;

function coerce(value: ExprLike): Expr {
  if (value instanceof Expr) {
    return value;
  }
  if (typeof value === "string") {
    return lit(value);
  }
  if (typeof value === "boolean") {
    return litBool(value);
  }
  if (typeof value === "number") {
    return lit(value);
  }
  throw new TypeError(`Unsupported literal type: ${typeof value}`);
}

/**
 * A SQL expression backed by the shared LanceDB expression builder.
 *
 * @example
 * ```ts
 * table.query().where(col("firstName").eq("Alice")).toArray();
 * ```
 */
export class Expr {
  inner: NativeExpr;

  constructor(inner: NativeExpr) {
    this.inner = inner;
  }

  /** Compare for equality. */
  eq(other: ExprLike): Expr {
    return new Expr(this.inner.eq(coerce(other).inner));
  }

  /** Compare for inequality. */
  ne(other: ExprLike): Expr {
    return new Expr(this.inner.ne(coerce(other).inner));
  }

  /** Compare for less than. */
  lt(other: ExprLike): Expr {
    return new Expr(this.inner.lt(coerce(other).inner));
  }

  /** Compare for less than or equal to. */
  lte(other: ExprLike): Expr {
    return new Expr(this.inner.lte(coerce(other).inner));
  }

  /** Compare for greater than. */
  gt(other: ExprLike): Expr {
    return new Expr(this.inner.gt(coerce(other).inner));
  }

  /** Compare for greater than or equal to. */
  gte(other: ExprLike): Expr {
    return new Expr(this.inner.gte(coerce(other).inner));
  }

  /** Combine two predicates with AND. */
  and(other: Expr): Expr {
    return new Expr(this.inner.and(other.inner));
  }

  /** Combine two predicates with OR. */
  or(other: Expr): Expr {
    return new Expr(this.inner.or(other.inner));
  }

  /** Negate this predicate. */
  not(): Expr {
    return new Expr(this.inner.not());
  }

  /** Add a value. */
  add(other: ExprLike): Expr {
    return new Expr(this.inner.add(coerce(other).inner));
  }

  /** Subtract a value. */
  sub(other: ExprLike): Expr {
    return new Expr(this.inner.sub(coerce(other).inner));
  }

  /** Multiply by a value. */
  mul(other: ExprLike): Expr {
    return new Expr(this.inner.mul(coerce(other).inner));
  }

  /** Divide by a value. */
  div(other: ExprLike): Expr {
    return new Expr(this.inner.div(coerce(other).inner));
  }

  /** Convert a string to lowercase. */
  lower(): Expr {
    return new Expr(this.inner.lower());
  }

  /** Convert a string to uppercase. */
  upper(): Expr {
    return new Expr(this.inner.upper());
  }

  /** Test whether a string contains a substring. */
  contains(substr: ExprLike): Expr {
    return new Expr(this.inner.contains(coerce(substr).inner));
  }

  /** Test membership in a list of values. */
  isIn(values: ExprLike[]): Expr {
    return new Expr(this.inner.isIn(values.map((v) => coerce(v).inner)));
  }

  /** Cast to an Arrow data type, such as "Int64". */
  cast(dataType: string): Expr {
    return new Expr(this.inner.cast(dataType));
  }

  /** Serialize with the LanceDB SQL dialect, preserving column case. */
  toSql(): string {
    return this.inner.toSql();
  }
}

/** Reference a column by its exact, case-sensitive name. */
export function col(name: string): Expr {
  return new Expr(nativeCol(name));
}

/** Create a string or numeric literal. Unsafe integers are represented as Float64. */
export function lit(value: string | number): Expr {
  if (typeof value === "string") {
    return new Expr(nativeLit(value));
  }
  if (Number.isSafeInteger(value)) {
    return new Expr(nativeLitInt(value));
  }
  return new Expr(nativeLit(value));
}

/** Create a boolean literal. */
export function litBool(value: boolean): Expr {
  return new Expr(nativeLitBool(value));
}

/** Create an Int64 literal from a JavaScript safe integer. Other numbers throw RangeError. */
export function litInt(value: number): Expr {
  if (!Number.isSafeInteger(value)) {
    throw new RangeError("Integer literals must be safe integers");
  }
  return new Expr(nativeLitInt(value));
}

/** Call a registered SQL function, such as `func("lower", col("firstName"))`. */
export function func(name: string, ...args: ExprLike[]): Expr {
  return new Expr(
    nativeFunc(
      name,
      args.map((a) => coerce(a).inner),
    ),
  );
}
