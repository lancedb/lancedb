// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Datafusion user defined functions

use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use arrow_schema::DataType;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::prelude::SessionContext;
use datafusion_functions::utils::make_scalar_function;
use std::sync::{Arc, LazyLock};

pub mod json;

/// Register UDF functions to datafusion context.
pub fn register_functions(ctx: &SessionContext) {
    ctx.register_udf(CONTAINS_TOKENS_UDF.clone());
    // JSON functions
    ctx.register_udf(json::json_extract_udf());
    ctx.register_udf(json::json_extract_with_type_udf());
    ctx.register_udf(json::json_exists_udf());
    ctx.register_udf(json::json_get_udf());
    ctx.register_udf(json::json_get_string_udf());
    ctx.register_udf(json::json_get_int_udf());
    ctx.register_udf(json::json_get_float_udf());
    ctx.register_udf(json::json_get_bool_udf());
    ctx.register_udf(json::json_array_contains_udf());
    ctx.register_udf(json::json_array_length_udf());
    // GEO functions
    #[cfg(feature = "geo")]
    lance_geo::register_functions(ctx);
    #[cfg(not(feature = "geo"))]
    register_geo_stub_functions(ctx);
}

/// When the `geo` feature is disabled, register stub UDFs for spatial SQL functions
/// so that users get a clear error mentioning the feature flag instead of
/// DataFusion's generic "Unknown function" error.
#[cfg(not(feature = "geo"))]
fn register_geo_stub_functions(ctx: &SessionContext) {
    let geo_funcs = [
        "st_intersects",
        "st_contains",
        "st_within",
        "st_touches",
        "st_crosses",
        "st_overlaps",
        "st_covers",
        "st_coveredby",
        "st_distance",
        "st_area",
        "st_length",
    ];

    for name in geo_funcs {
        let func_name = name.to_string();
        let stub = Arc::new(make_scalar_function(
            move |_args: &[ArrayRef]| {
                Err(datafusion::error::DataFusionError::Plan(format!(
                    "Function '{}' requires the `geo` feature. \
                     Rebuild with `--features geo` to enable geospatial functions.",
                    func_name
                )))
            },
            vec![],
        ));

        ctx.register_udf(create_udf(
            name,
            vec![DataType::Binary, DataType::Binary],
            DataType::Boolean,
            Volatility::Immutable,
            stub,
        ));
    }
}

/// This method checks whether a string contains all specified tokens. The tokens are separated by
/// punctuations and white spaces.
///
/// The functionality is equivalent to FTS MatchQuery (with fuzziness disabled, Operator::And,
/// and using the simple tokenizer). If FTS index exists and suites the query, it will be used to
/// optimize the query.
///
/// Usage
/// * Use `contains_tokens` in sql.
/// ```rust,ignore
/// let sql = "SELECT * FROM table WHERE contains_tokens(text_col, 'fox jumps dog')";
/// let mut ds = Dataset::open(&ds_path).await?;
/// let ctx = SessionContext::new();
/// ctx.register_table(
///     "table",
///     Arc::new(LanceTableProvider::new(dataset, false, false)),
/// )?;
/// register_functions(&ctx);
/// let df = ctx.sql(sql).await?;
/// ```
fn contains_tokens() -> ScalarUDF {
    let function = Arc::new(make_scalar_function(
        |args: &[ArrayRef]| {
            let column = args[0].as_any().downcast_ref::<StringArray>().ok_or(
                datafusion::error::DataFusionError::Execution(
                    "First argument of contains_tokens can't be cast to string".to_string(),
                ),
            )?;
            let scalar_str = args[1].as_any().downcast_ref::<StringArray>().ok_or(
                datafusion::error::DataFusionError::Execution(
                    "Second argument of contains_tokens can't be cast to string".to_string(),
                ),
            )?;

            let tokens: Option<Vec<&str>> = match scalar_str.len() {
                0 => None,
                _ => Some(collect_tokens(scalar_str.value(0))),
            };

            let result = column.iter().map(|text| {
                text.map(|text| {
                    let text_tokens = collect_tokens(text);
                    if let Some(tokens) = &tokens {
                        tokens.len()
                            == tokens
                                .iter()
                                .filter(|token| text_tokens.contains(*token))
                                .count()
                    } else {
                        true
                    }
                })
            });

            Ok(Arc::new(BooleanArray::from_iter(result)) as ArrayRef)
        },
        vec![],
    ));

    create_udf(
        "contains_tokens",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        function,
    )
}

/// Split tokens separated by punctuations and white spaces.
fn collect_tokens(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

pub static CONTAINS_TOKENS_UDF: LazyLock<ScalarUDF> = LazyLock::new(contains_tokens);

#[cfg(test)]
mod tests {
    use crate::udf::CONTAINS_TOKENS_UDF;
    use arrow_array::{Array, BooleanArray, StringArray};
    use arrow_schema::{DataType, Field};
    use datafusion::logical_expr::ScalarFunctionArgs;
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_contains_tokens() {
        // Prepare arguments
        let contains_tokens = CONTAINS_TOKENS_UDF.clone();
        let text_col = Arc::new(StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat catchup fish",
            "cat fish catch",
        ]));
        let token = Arc::new(StringArray::from(vec![
            " cat catch fish.",
            " cat catch fish.",
            " cat catch fish.",
            " cat catch fish.",
            " cat catch fish.",
        ]));

        let args = vec![ColumnarValue::Array(text_col), ColumnarValue::Array(token)];
        let arg_fields = vec![
            Arc::new(Field::new("text_col".to_string(), DataType::Utf8, false)),
            Arc::new(Field::new("token".to_string(), DataType::Utf8, false)),
        ];

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: 5,
            return_field: Arc::new(Field::new("res".to_string(), DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };

        // Invoke contains_tokens manually
        let values = contains_tokens.invoke_with_args(args).unwrap();

        if let ColumnarValue::Array(array) = values {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                array.clone(),
                BooleanArray::from(vec![true, true, true, false, true])
            );
        } else {
            panic!("Expected an Array but got {:?}", values);
        }
    }
}
