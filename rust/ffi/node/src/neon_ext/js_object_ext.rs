// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::error::{Error, Result};
use neon::prelude::*;

// extends neon's [JsObject] with helper functions to extract properties
pub trait JsObjectExt {
    fn get_opt_u32(&self, cx: &mut FunctionContext, key: &str) -> Result<Option<u32>>;
    fn get_usize(&self, cx: &mut FunctionContext, key: &str) -> Result<usize>;
    #[allow(dead_code)]
    fn get_opt_usize(&self, cx: &mut FunctionContext, key: &str) -> Result<Option<usize>>;
}

impl JsObjectExt for JsObject {
    fn get_opt_u32(&self, cx: &mut FunctionContext, key: &str) -> Result<Option<u32>> {
        let val_opt = self
            .get_opt::<JsNumber, _, _>(cx, key)?
            .map(|s| f64_to_u32_safe(s.value(cx), key));
        val_opt.transpose()
    }

    fn get_usize(&self, cx: &mut FunctionContext, key: &str) -> Result<usize> {
        let val = self.get::<JsNumber, _, _>(cx, key)?.value(cx);
        f64_to_usize_safe(val, key)
    }

    fn get_opt_usize(&self, cx: &mut FunctionContext, key: &str) -> Result<Option<usize>> {
        let val_opt = self
            .get_opt::<JsNumber, _, _>(cx, key)?
            .map(|s| f64_to_usize_safe(s.value(cx), key));
        val_opt.transpose()
    }
}

fn f64_to_u32_safe(n: f64, key: &str) -> Result<u32> {
    use conv::*;

    n.approx_as::<u32>().map_err(|e| match e {
        FloatError::NegOverflow(_) => Error::OutOfRange {
            name: key.into(),
            message: "must be > 0".to_string(),
        },
        FloatError::PosOverflow(_) => Error::OutOfRange {
            name: key.into(),
            message: format!("must be < {}", u32::MAX),
        },
        FloatError::NotANumber(_) => Error::OutOfRange {
            name: key.into(),
            message: "not a valid number".to_string(),
        },
    })
}

fn f64_to_usize_safe(n: f64, key: &str) -> Result<usize> {
    use conv::*;

    n.approx_as::<usize>().map_err(|e| match e {
        FloatError::NegOverflow(_) => Error::OutOfRange {
            name: key.into(),
            message: "must be > 0".to_string(),
        },
        FloatError::PosOverflow(_) => Error::OutOfRange {
            name: key.into(),
            message: format!("must be < {}", usize::MAX),
        },
        FloatError::NotANumber(_) => Error::OutOfRange {
            name: key.into(),
            message: "not a valid number".to_string(),
        },
    })
}
