import { no } from "zod/v4/locales";
import { type IntoSql, toSQL as valueToSQL } from "./util"; 

export type binaryOp =  "=" | "!=" | "<" | "<=" | ">=" | ">" | "AND" | "OR" | "+" | "*" | "-" | "/" | "%" | "LIKE"

type unaryOp =  "NOT" | "-" 

type ExprNode = | {kind: "column"; name:string} | {kind: "literal"; value: IntoSql} | {kind:"binary"; left:Expr; op: binaryOp ; right: Expr}
| {kind: "unary"; op: unaryOp; operand: Expr} |{kind: "function" ; name: string; args: Expr[]} | {kind: "cast"; expr: Expr; dataType:string}
| {kind: "isIn"; expr: Expr; values: Expr[] ;negated: boolean} | {kind: "alias"; expr: Expr; name:string} | {kind: "between"; expr:Expr ;low:Expr ; high: Expr ; negated:boolean}
| {kind: "isNull" ; expr: Expr ; negated: boolean}

export class Expr{
  readonly _node : ExprNode;

  constructor(node: ExprNode){
    this._node = node;
  }

  // COMPARISON OPERATIONS
  eq(other: IntoExpr){
    return new Expr({kind: "binary", left: this, op: "=", right: intoExpr(other)});
  }

  neq(other: IntoExpr){
    return new Expr({kind: "binary", left: this, op: "!=", right: intoExpr(other)});
  }

    lt(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: "<",
      right: intoExpr(other),
    });
  }
 
  lte(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: "<=",
      right: intoExpr(other),
    });
  }
 
  gt(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: ">",
      right: intoExpr(other),
    });
  }
 
  gte(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: ">=",
      right: intoExpr(other),
    });
  }

  //BOOLEAN LOGIC 
  and(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: "AND",
      right: intoExpr(other),
    });
  }
 
  or(other: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: "OR",
      right: intoExpr(other),
    });
  }
 
  not(): Expr {
    return new Expr({ kind: "unary", op: "NOT", operand: this });
  }

  //ARITHMETIC & GENERAL BINARY OPERATIONS
  op(operation: binaryOp, other: IntoExpr): Expr{
    return new Expr({kind: "binary", left: this, op: operation, right: intoExpr(other)});
  }
  
 //Kept neg since its unary operation
  neg(): Expr {
    return new Expr({ kind: "unary", op: "-", operand: this});
  }

  //STRING OPERATIONS
  lower(): Expr {
    return new Expr({ kind: "function", name: "lower", args: [this] });
  }
 
  upper(): Expr {
    return new Expr({ kind: "function", name: "upper", args: [this] });
  }
 
  like(pattern: IntoExpr): Expr {
    return new Expr({
      kind: "binary",
      left: this,
      op: "LIKE",
      right: intoExpr(pattern),
    });
  }

  contains(substr: IntoExpr): Expr {
    return func("strpos", this, intoExpr(substr)).gt(lit(0));
  }
 
  startsWith(prefix: IntoExpr): Expr {
    return func("starts_with", this, intoExpr(prefix));
  }
 
  endsWith(suffix: IntoExpr): Expr {
    return func("ends_with", this, intoExpr(suffix));
  }
 
  length(): Expr {
    return func("length", this);
  }
 
  trim(): Expr {
    return func("trim", this);
  }

  //NULL CHECKS
  isNull(): Expr {
    return new Expr({ kind: "isNull", expr: this, negated: false });
  }
 
  isNotNull(): Expr {
    return new Expr({ kind: "isNull", expr: this, negated: true });
  }

  //BINARY MEMBERSHIP CHECKS
  isIn(values: IntoExpr[]): Expr {
    return new Expr({
      kind: "isIn",
      expr: this,
      values: values.map(intoExpr),
      negated: false,
    });
  }
 
  isNotIn(values: IntoExpr[]): Expr {
    return new Expr({
      kind: "isIn",
      expr: this,
      values: values.map(intoExpr),
      negated: true,
    });
  }
 
  between(low: IntoExpr, high: IntoExpr): Expr {
    return new Expr({
      kind: "between",
      expr: this,
      low: intoExpr(low),
      high: intoExpr(high),
      negated: false,
    });
  }
  notBetween(low: IntoExpr, high: IntoExpr): Expr {
    return new Expr({
      kind: "between",
      expr: this,
      low: intoExpr(low),
      high: intoExpr(high),
      negated: true,
    });
  }
  //TYPECASTING FUNCTION
  cast(dataType: string): Expr {
    return new Expr({ kind: "cast", expr: this, dataType });
  }
  //ALIAS FOR AN EXPRESSION
  alias(name: string): Expr {
    return new Expr({ kind: "alias", expr: this, name });
  }

  //Specifically for user-debugging
  toSQL(): string{
    return nodeToSQL(this._node);
  }

}

export function col(name: string) : Expr{
  return new Expr({kind: "column", name});
}

export function lit(value: IntoSql): Expr{
  return new Expr({kind: "literal", value});
}
//Functions not covered by Expr
export function func(name: string, ...args: IntoExpr[]) : Expr{
  return new Expr({kind: "function", name, args: args.map(intoExpr)});
}


export function exprToSQL(value: Expr | string): string {
  if (typeof value === "string") {
    return value;
  }
  return nodeToSQL(value._node);
}
//Function to convert from our expressions to SQL
function nodeToSQL(node : ExprNode): string{
  switch(node.kind){
    case "column": return `"${node.name.replace(/"/g, '""')}"`;

    case "literal": return valueToSQL(node.value);       //referencing util.ts

    case "binary": {
      // Helper function below in case there is order of operations necessary and we need add paranthesis
      const l = wrapSQL(node.op, node.left);
      const r = wrapSQL(node.op, node.right);
      return `${l} ${node.op} ${r}`
    }

    case "unary":
      if (node.op === "-") {
        // Wrapping it in paranthesis to avoid ambiguity ex: -(a)
        return `(-${nodeToSQL(node.operand._node)})`;
      }
      // NOT — no parens needed
      return `NOT ${nodeToSQL(node.operand._node)}`;
    
    case "function": return `${node.name}(${node.args.map((a) => nodeToSQL(a._node)).join(", ")})`;

    case "cast": return `CAST(${nodeToSQL(node.expr._node)} AS ${node.dataType})`;

    case "isIn": {
      const vals = node.values.map((v) => nodeToSQL(v._node)).join(", ");
      const notStr = node.negated ? " NOT" : "";
      return `${nodeToSQL(node.expr._node)}${notStr} IN (${vals})`;
    }

    case "isNull": {
      const suffix = node.negated ? " IS NOT NULL" : " IS NULL";
      return `${nodeToSQL(node.expr._node)}${suffix}`;
    }

    case "between": {
      const notStr = node.negated ? " NOT" : "";
      return `${nodeToSQL(node.expr._node)}${notStr} BETWEEN ${nodeToSQL(node.low._node)} AND ${nodeToSQL(node.high._node)}`;
    }
 
    case "alias": return `${nodeToSQL(node.expr._node)} AS "${node.name.replace(/"/g, '""')}"`;
  }
}

function wrapSQL(parentOp: binaryOp, child: Expr): string {
  const sql = nodeToSQL(child._node);
  return needsParens(parentOp, child._node) ? `(${sql})` : sql;
}

//Helper function to determine operation priority 
function needsParens(parentOp: binaryOp, childNode: ExprNode): boolean {
  if (childNode.kind !== "binary") return false;
 
  const precedence: Record<binaryOp, number> = {
    OR: 1,
    AND: 2,
    "=": 3,
    "!=": 3,
    "<": 3,
    "<=": 3,
    ">": 3,
    ">=": 3,
    LIKE: 3,
    "+": 4,
    "-": 4,
    "*": 5,
    "/": 5,
    "%": 5,
  };
 
  const parentPrec = precedence[parentOp];
  const childPrec = precedence[childNode.op];
 
  if (childPrec < parentPrec) return true;
  if (childPrec === parentPrec && parentPrec === 3) return true; //comparisons
  return false;
}

export type IntoExpr = Expr | IntoSql;
// so users don't have to use literal everytime for Intosql values for example:
// it simplifies col("age").gt(lit(30)) to col("age").gt(30)
function intoExpr(value: IntoExpr){
  if (value instanceof Expr){
    return value;
  }
  else{
    return lit(value);
  }
}

