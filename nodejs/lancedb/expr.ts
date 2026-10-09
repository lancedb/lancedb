// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { Expr as NativeExpr, col as nativeCol, lit as nativeLit, litBool as nativeLitBool, litInt as nativeLitInt, func as nativeFunc } from "./native.js";

export { Expr as NativeExpr } from "./native";

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
    if (Number.isInteger(value)) {
      return litInt(value);
    }
    return lit(value);
  }
  throw new TypeError(`Unsupported literal type: ${typeof value}`);
}

export class Expr {
  inner: NativeExpr;

  constructor(inner: NativeExpr) {
    this.inner = inner;
  }

  eq(other: ExprLike): Expr {
    return new Expr(this.inner.eq(coerce(other).inner));
  }

  ne(other: ExprLike): Expr {
    return new Expr(this.inner.ne(coerce(other).inner));
  }

  lt(other: ExprLike): Expr {
    return new Expr(this.inner.lt(coerce(other).inner));
  }

  lte(other: ExprLike): Expr {
    return new Expr(this.inner.lte(coerce(other).inner));
  }

  gt(other: ExprLike): Expr {
    return new Expr(this.inner.gt(coerce(other).inner));
  }

  gte(other: ExprLike): Expr {
    return new Expr(this.inner.gte(coerce(other).inner));
  }

  and(other: Expr): Expr {
    return new Expr(this.inner.and(other.inner));
  }

  or(other: Expr): Expr {
    return new Expr(this.inner.or(other.inner));
  }

  not(): Expr {
    return new Expr(this.inner.not());
  }

  add(other: ExprLike): Expr {
    return new Expr(this.inner.add(coerce(other).inner));
  }

  sub(other: ExprLike): Expr {
    return new Expr(this.inner.sub(coerce(other).inner));
  }

  mul(other: ExprLike): Expr {
    return new Expr(this.inner.mul(coerce(other).inner));
  }

  div(other: ExprLike): Expr {
    return new Expr(this.inner.div(coerce(other).inner));
  }

  lower(): Expr {
    return new Expr(this.inner.lower());
  }

  upper(): Expr {
    return new Expr(this.inner.upper());
  }

  contains(substr: ExprLike): Expr {
    return new Expr(this.inner.contains(coerce(substr).inner));
  }

  isIn(values: ExprLike[]): Expr {
    return new Expr(this.inner.isIn(values.map((v) => coerce(v).inner)));
  }

  cast(dataType: string): Expr {
    return new Expr(this.inner.cast(dataType));
  }

  toSql(): string {
    return this.inner.toSql();
  }
}

export function col(name: string): Expr {
  return new Expr(nativeCol(name));
}

export function lit(value: string | number): Expr {
  if (typeof value === "string") {
    return new Expr(nativeLit(value));
  }
  if (Number.isInteger(value)) {
    return new Expr(nativeLitInt(value));
  }
  return new Expr(nativeLit(value));
}

export function litBool(value: boolean): Expr {
  return new Expr(nativeLitBool(value));
}

export function litInt(value: number): Expr {
  return new Expr(nativeLitInt(value));
}

export function func(name: string, ...args: ExprLike[]): Expr {
  return new Expr(nativeFunc(name, args.map((a) => coerce(a).inner)));
}
