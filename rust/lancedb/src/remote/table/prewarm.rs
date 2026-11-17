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
