// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Napi bindings for the LanceDB expression builder API.

use std::ops::{Add, Div, Mul, Not, Sub};

use lancedb::expr::{
    DfExpr, col as ldb_col, contains, expr_cast, is_in, lit as df_lit, lower, upper,
};
use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
#[derive(Clone)]
pub struct Expr {
    pub(crate) inner: DfExpr,
}

#[napi]
impl Expr {
    #[napi]
    pub fn eq(&self, other: &Expr) -> Self {
        Self(self.inner.clone().eq(other.inner.clone()))
    }

    #[napi]
    pub fn ne(&self, other: &Expr) -> Self {
        Self(self.inner.clone().not_eq(other.inner.clone()))
    }

    #[napi]
    pub fn lt(&self, other: &Expr) -> Self {
        Self(self.inner.clone().lt(other.inner.clone()))
    }

    #[napi]
    pub fn lte(&self, other: &Expr) -> Self {
        Self(self.inner.clone().lt_eq(other.inner.clone()))
    }

    #[napi]
    pub fn gt(&self, other: &Expr) -> Self {
        Self(self.inner.clone().gt(other.inner.clone()))
    }

    #[napi]
    pub fn gte(&self, other: &Expr) -> Self {
        Self(self.inner.clone().gt_eq(other.inner.clone()))
    }

    #[napi]
    pub fn and(&self, other: &Expr) -> Self {
        Self(self.inner.clone().and(other.inner.clone()))
    }

    #[napi]
    pub fn or(&self, other: &Expr) -> Self {
        Self(self.inner.clone().or(other.inner.clone()))
    }

    #[napi]
    pub fn not(&self) -> Self {
        Self(self.inner.clone().not())
    }

    #[napi]
    pub fn add(&self, other: &Expr) -> Self {
        Self(self.inner.clone().add(other.inner.clone()))
    }

    #[napi]
    pub fn sub(&self, other: &Expr) -> Self {
        Self(self.inner.clone().sub(other.inner.clone()))
    }

    #[napi]
    pub fn mul(&self, other: &Expr) -> Self {
        Self(self.inner.clone().mul(other.inner.clone()))
    }

    #[napi]
    pub fn div(&self, other: &Expr) -> Self {
        Self(self.inner.clone().div(other.inner.clone()))
    }

    #[napi]
    pub fn lower(&self) -> Self {
        Self(lower(self.inner.clone()))
    }

    #[napi]
    pub fn upper(&self) -> Self {
        Self(upper(self.inner.clone()))
    }

    #[napi]
    pub fn contains(&self, substr: &Expr) -> Self {
        Self(contains(self.inner.clone(), substr.inner.clone()))
    }

    #[napi]
    pub fn is_in(&self, list: Vec<&Expr>) -> Self {
        let items: Vec<DfExpr> = list.into_iter().map(|e| e.inner.clone()).collect();
        Self(is_in(self.inner.clone(), items))
    }

    #[napi]
    pub fn cast(&self, data_type: String) -> napi::Result<Self> {
        let arrow_type = data_type
            .parse::<lancedb::arrow::datatypes::DataType>()
            .map_err(|e| napi::Error::from_reason(format!("Invalid data type: {}", e)))?;
        Ok(Self(expr_cast(self.inner.clone(), arrow_type)))
    }

    #[napi]
    pub fn to_sql(&self) -> napi::Result<String> {
        lancedb::expr::expr_to_sql_string(&self.inner)
            .map_err(|e| napi::Error::from_reason(e.to_string()))
    }
}

/// Create a column reference expression.
///
/// The column name is preserved exactly as given (case-sensitive), so
/// `col("firstName")` correctly references a field named `firstName`.
#[napi]
pub fn col(name: String) -> Expr {
    Expr {
        inner: ldb_col(name),
    }
}

/// Create a literal (constant) value expression.
#[napi]
pub fn lit(value: napi::bindgen_prelude::Either<String, f64>) -> napi::Result<Expr> {
    match value {
        Either::A(s) => Ok(Expr {
            inner: df_lit(s),
        }),
        Either::B(f) => Ok(Expr {
            inner: df_lit(f),
        }),
    }
}

/// Create a literal boolean value expression.
#[napi]
pub fn lit_bool(value: bool) -> Expr {
    Expr {
        inner: df_lit(value),
    }
}

/// Create a literal integer value expression.
#[napi]
pub fn lit_int(value: i64) -> Expr {
    Expr {
        inner: df_lit(value),
    }
}

/// Call an arbitrary registered SQL function by name.
#[napi]
pub fn func(name: String, args: Vec<&Expr>) -> napi::Result<Expr> {
    let df_args: Vec<DfExpr> = args.into_iter().map(|e| e.inner.clone()).collect();
    lancedb::expr::func(&name, df_args)
        .map(|inner| Expr { inner })
        .map_err(|e| napi::Error::from_reason(e.to_string()))
}
