// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::error::{Error, Result};
use neon::prelude::*;

// extends neon's [JsObject] with helper functions to extract properties
pub trait JsObjectExt {
    fn get_opt_u32(&self, cx: &mut FunctionContext, key: &str) -> Result<Option<u32>>;
    fn get_usize(&self, cx: &mut FunctionContext, key: &str) -> Result<usize>;
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
