// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL submission and receipt validation for data-cache prewarm jobs.

use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use datafusion_sql::sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::Token};

use super::{quote_sql_identifier, quote_sql_string};
use crate::{Error, Result, table::PrewarmDataOptions};

pub(super) fn statement(
    table: &str,
    branch: Option<&str>,
    options: &PrewarmDataOptions,
) -> Result<String> {
    let invalid = |message: &str| Error::InvalidInput {
        message: message.into(),
    };
    if options.concurrency == Some(0) {
        return Err(invalid("prewarm concurrency must be greater than 0"));
    }
    if let Some(filter) = &options.filter {
        if filter.trim().is_empty() {
            return Err(invalid("prewarm filter must not be empty"));
        }
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect)
            .try_with_sql(filter)
            .map_err(|error| invalid(&format!("invalid prewarm filter: {error}")))?;
        parser
            .parse_expr()
            .map_err(|error| invalid(&format!("invalid prewarm filter: {error}")))?;
        if parser.peek_token().token != Token::EOF {
            return Err(invalid("prewarm filter must be a single SQL expression"));
        }
    }
    let mut settings = Vec::new();
    if let Some(branch) = branch {
        if branch.trim().is_empty() {
            return Err(invalid("prewarm branch must not be empty"));
        }
        settings.push(format!("branch = {}", quote_sql_string(branch)));
    }
    if let Some(concurrency) = options.concurrency {
        settings.push(format!("concurrency = {concurrency}"));
    }
    let mut sql = "PREWARM".to_string();
    if !settings.is_empty() {
        sql.push_str(&format!(" WITH ({})", settings.join(", ")));
    }
    let mut columns = Vec::new();
    for column in &options.columns {
        let parts = lance::datatypes::parse_field_path(column)?;
        let column = parts
            .iter()
            .map(|part| quote_sql_identifier(part))
            .collect::<Vec<_>>()
            .join(".");
        if !columns.contains(&column) {
            columns.push(column);
        }
    }
    let projection = if columns.is_empty() {
        "*".into()
    } else {
        columns.join(", ")
    };
    sql.push_str(&format!(
        " AS SELECT {projection} FROM {}",
        quote_sql_identifier(table)
    ));
    if let Some(filter) = &options.filter {
        sql.push_str(&format!(" WHERE {filter}"));
    }
    Ok(sql)
}

pub(super) fn receipt(batches: &[RecordBatch]) -> Result<(String, u64)> {
    let invalid = || {
        Error::Runtime {
        message: "PREWARM did not return a single (job_id, table_version) receipt; inspect jobs before retrying because work may already have been submitted".into(),
    }
    };
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1 {
        return Err(invalid());
    }
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .ok_or_else(invalid)?;
    let id = batch
        .column_by_name("job_id")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    let version = batch
        .column_by_name("table_version")
        .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(invalid)?;
    if id.is_null(0) || id.value(0).is_empty() || version.is_null(0) {
        return Err(invalid());
    }
    Ok((id.value(0).into(), version.value(0)))
}

/// Decode the versioned JSON payload without depending on the display text.
pub(super) fn plan(batches: &[RecordBatch]) -> Result<crate::table::PrewarmDataPlan> {
    let invalid = || {
        Error::Runtime {
        message: "EXPLAIN PREWARM did not return a valid structured preview; a server supporting EXPLAIN (FORMAT JSON) PREWARM is required".into(),
    }
    };
    if batches.iter().map(RecordBatch::num_rows).sum::<usize>() != 1 {
        return Err(invalid());
    }
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() == 1)
        .ok_or_else(invalid)?;
    let payload = batch
        .column_by_name("plan")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(invalid)?;
    if payload.is_null(0) {
        return Err(invalid());
    }
    #[derive(serde::Deserialize)]
    struct Preview {
        format_version: u32,
        #[serde(flatten)]
        plan: crate::table::PrewarmDataPlan,
    }
    let preview: Preview = serde_json::from_str(payload.value(0)).map_err(|_| invalid())?;
    if preview.format_version != 1 {
        return Err(invalid());
    }
    Ok(preview.plan)
}

pub(super) fn map_explain_error(error: Error) -> Error {
    fn unsupported_format(error: &Error) -> bool {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } => {
                let diagnostic = message.to_ascii_lowercase();
                // Older servers hand this EXPLAIN to DataFusion, which rejects
                // JSON as a configuration error (sometimes gRPC Internal).
                (*grpc_code == tonic::Code::Internal as i32
                    || *grpc_code == tonic::Code::InvalidArgument as i32)
                    && diagnostic.contains("invalid explain format.")
                    && diagnostic.contains("got 'json'")
            }
            Error::JobFailed { failure, .. } => {
                failure.source.as_deref().is_some_and(unsupported_format)
            }
            _ => false,
        }
    }
    let error = if unsupported_format(&error) {
        Error::NotSupported {
            message: error.to_string(),
        }
    } else {
        map_error(error)
    };
    match error {
        Error::NotSupported { message } => Error::NotSupported {
            message: format!(
                "Data prewarm previews require a server supporting EXPLAIN (FORMAT JSON) PREWARM; upgrade the server. Preview will not fall back to text or REST. Server diagnostic: {message}"
            ),
        },
        other => other,
    }
}

pub(super) fn map_error(error: Error) -> Error {
    fn unsupported(error: &Error) -> Option<&Error> {
        match error {
            Error::Sql {
                message, grpc_code, ..
            } => {
                let diagnostic = message.to_ascii_lowercase();
                (*grpc_code == tonic::Code::Unimplemented as i32
                    || (*grpc_code == tonic::Code::InvalidArgument as i32
                        && (diagnostic.contains("found: prewarm")
                            || diagnostic.contains("supported syntax: prewarm")
                            || diagnostic.contains("unknown prewarm option"))))
                .then_some(error)
            }
            Error::JobFailed { failure, .. } => failure.source.as_deref().and_then(unsupported),
            _ => None,
        }
    }
    if let Some(source) = unsupported(&error) {
        let diagnostic = if matches!(error, Error::Sql { .. }) {
            error.to_string()
        } else {
            format!("{error}; {source}")
        };
        Error::NotSupported {
            message: format!(
                "Data prewarm requires a compatible SQL PREWARM server; upgrade the server. Requested options will not be removed and submission will not fall back to REST. Server diagnostic: {diagnostic}"
            ),
        }
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn preview_maps_old_explain_format_errors_without_masking_auth_or_filters() {
        for (code, message, unsupported) in [
            (
                tonic::Code::Internal,
                "Invalid or Unsupported Configuration: Invalid explain format. Expected 'indent', 'tree', 'pgjson' or 'graphviz'. Got 'JSON'",
                true,
            ),
            (
                tonic::Code::InvalidArgument,
                "Invalid explain format. Got 'JSON'",
                true,
            ),
            (
                tonic::Code::PermissionDenied,
                "Invalid explain format. Got 'JSON'",
                false,
            ),
            (
                tonic::Code::InvalidArgument,
                "invalid filter expression",
                false,
            ),
        ] {
            let error = map_explain_error(Error::Sql {
                message: message.into(),
                request_id: "preview-request".into(),
                grpc_code: code as i32,
                metadata: Default::default(),
            });
            assert_eq!(
                matches!(error, Error::NotSupported { .. }),
                unsupported,
                "{error}"
            );
            assert!(error.to_string().contains(message));
            assert!(error.to_string().contains("preview-request"));
            if unsupported {
                assert!(error.to_string().contains("EXPLAIN (FORMAT JSON) PREWARM"));
            }
        }
    }

    #[test]
    fn preview_requires_one_versioned_typed_payload() {
        let valid = serde_json::json!({
            "format_version": 1, "table_version": 42, "plan": "display only",
            "pruning": {"mode":"future_mode", "input_fragments":3,
                "selected_fragments":0, "pruned_fragments":3, "selected_row_ranges":0},
            "future_field": true,
        });
        let batch = |value: &serde_json::Value| {
            RecordBatch::try_from_iter([(
                "plan",
                Arc::new(StringArray::from(vec![value.to_string()])) as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let result = plan(&[batch(&valid)]).unwrap();
        assert_eq!(result.table_version, 42);
        assert_eq!(result.pruning.mode, "future_mode");
        assert_eq!(result.pruning.selected_row_ranges, Some(0));
        assert_eq!(result.pruning.fallback_reason, None);
        for mutation in ["version", "missing", "negative", "text"] {
            let mut value = valid.clone();
            match mutation {
                "version" => value["format_version"] = 2.into(),
                "missing" => {
                    value.as_object_mut().unwrap().remove("pruning");
                }
                "negative" => value["pruning"]["selected_fragments"] = (-1).into(),
                _ => value = "legacy display text".into(),
            }
            assert!(plan(&[batch(&value)]).is_err(), "{mutation}");
        }
        assert!(plan(&[]).is_err());
        assert!(plan(&[batch(&valid), batch(&valid)]).is_err());
    }

    #[test]
    fn quotes_paths_branch_and_filter_without_changing_options() {
        let options = PrewarmDataOptions::new()
            .columns(["attrs.score", "`caption.text`", "attrs.score"])
            .filter("quality > 0.7 AND caption = 'it''s ready'")
            .concurrency(4);
        assert_eq!(
            statement("clip\"name", Some("o'brien"), &options).unwrap(),
            "PREWARM WITH (branch = 'o''brien', concurrency = 4) AS SELECT \"attrs\".\"score\", \"caption.text\" FROM \"clip\"\"name\" WHERE quality > 0.7 AND caption = 'it''s ready'"
        );
        assert_eq!(
            statement("clips", None, &Default::default()).unwrap(),
            "PREWARM AS SELECT * FROM \"clips\""
        );
        assert!(statement("clips", None, &PrewarmDataOptions::new().filter("-1 < id")).is_ok());
    }

    #[test]
    fn rejects_invalid_inputs_before_submission() {
        for options in [
            PrewarmDataOptions::new().filter(""),
            PrewarmDataOptions::new().filter("id > 0; DROP TABLE clips"),
            PrewarmDataOptions::new().filter("id > 0 LIMIT 1"),
            PrewarmDataOptions::new().filter("id >"),
            PrewarmDataOptions::new().columns(["attrs..score"]),
            PrewarmDataOptions::new().concurrency(0),
        ] {
            assert!(statement("clips", None, &options).is_err(), "{options:?}");
        }
    }

    fn batch(ids: Vec<Option<&str>>, versions: Vec<Option<u64>>) -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "job_id",
                Arc::new(StringArray::from(ids)) as arrow_array::ArrayRef,
            ),
            (
                "table_version",
                Arc::new(UInt64Array::from(versions)) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn receipt_requires_one_non_null_job_and_version() {
        assert_eq!(
            receipt(&[batch(vec![Some("job-1")], vec![Some(42)])]).unwrap(),
            ("job-1".into(), 42)
        );
        for batches in [
            vec![],
            vec![batch(vec![], vec![])],
            vec![batch(vec![None], vec![Some(42)])],
            vec![batch(vec![Some("")], vec![Some(42)])],
            vec![batch(vec![Some("job")], vec![None])],
            vec![batch(vec![Some("a"), Some("b")], vec![Some(1), Some(2)])],
            vec![
                batch(vec![Some("a")], vec![Some(1)]),
                batch(vec![Some("b")], vec![Some(2)]),
            ],
            vec![RecordBatch::new_empty(Arc::new(Schema::new(vec![
                Field::new("wrong", DataType::Utf8, false),
            ])))],
        ] {
            let error = receipt(&batches).unwrap_err().to_string();
            assert!(error.contains("inspect jobs before retrying"), "{error}");
        }
    }

    #[test]
    fn compatibility_errors_preserve_permission_and_predicate_diagnostics() {
        for (code, message, unsupported) in [
            (
                tonic::Code::InvalidArgument,
                "ParserError: found: PREWARM",
                true,
            ),
            (
                tonic::Code::InvalidArgument,
                "Unknown PREWARM option 'branch'",
                true,
            ),
            (tonic::Code::PermissionDenied, "permission denied", false),
            (
                tonic::Code::InvalidArgument,
                "ParserError: expected expression, found unsupported_filter",
                false,
            ),
        ] {
            let error = map_error(Error::Sql {
                message: message.into(),
                request_id: "prewarm-request".into(),
                grpc_code: code as i32,
                metadata: Default::default(),
            });
            assert_eq!(matches!(error, Error::NotSupported { .. }), unsupported);
            assert!(error.to_string().contains(message));
            assert!(error.to_string().contains("prewarm-request"));
        }
    }
}
