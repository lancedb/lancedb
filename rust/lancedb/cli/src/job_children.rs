// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::formatter::{OutputFormat, ResultFormatter};
use crate::job::connection;
use crate::server_config::ConnectionArgs;
use anyhow::Result;
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use clap::Args;
use lancedb::job::details::{
    JobChildrenPage as ListJobChildrenResponse, JobChildrenRequest as ListJobChildrenRequest,
};
use std::io::Write;
use std::sync::Arc;

#[derive(Args, Debug)]
pub struct ChildrenArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,
    #[arg(long)]
    pub job_id: String,
    /// Children per page, default 100, capped at 1000.
    #[arg(long)]
    pub limit: Option<u32>,
    #[arg(long)]
    pub page_token: Option<String>,
}
pub async fn run_command(args: ChildrenArgs, format: OutputFormat) -> Result<()> {
    let response = connection(args.connection)
        .await?
        .job_handle(&args.job_id)?
        .children(ListJobChildrenRequest {
            job_id: args.job_id,
            limit: args.limit,
            page_token: args.page_token,
        })
        .await?;
    write_children(
        &response,
        format,
        &mut std::io::stdout(),
        &mut std::io::stderr(),
    )
}
fn write_children(
    response: &ListJobChildrenResponse,
    format: OutputFormat,
    output: &mut impl Write,
    diagnostics: &mut impl Write,
) -> Result<()> {
    if matches!(format, OutputFormat::Json) {
        writeln!(output, "{}", serde_json::to_string_pretty(response)?)?;
        return Ok(());
    }
    let details = response
        .children
        .iter()
        .map(|child| serde_json::to_value(&child.details))
        .collect::<serde_json::Result<Vec<_>>>()?;
    let fields = vec![
        Field::new("job_id", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("summary", DataType::Utf8, true),
    ];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            response.children.iter().map(|c| c.job_id.as_str()),
        )),
        Arc::new(StringArray::from_iter_values(
            details.iter().map(|d| d["kind"].as_str().unwrap()),
        )),
        Arc::new(StringArray::from_iter_values(
            details.iter().map(|d| d["summary"].to_string()),
        )),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    ResultFormatter::new(format).format_record_batches(&[batch], &mut *output)?;
    let metadata: &mut dyn Write = if matches!(format, OutputFormat::Table) {
        output
    } else {
        diagnostics
    };
    if let Some(progress) = &response.progress {
        writeln!(
            metadata,
            "Job progress: {} computed, {} skipped / {} planned rows",
            progress.rows_computed, progress.rows_skipped, progress.rows_planned
        )?;
    }
    writeln!(
        metadata,
        "History availability: {}",
        response.history_availability.as_str()
    )?;
    if let Some(token) = &response.page_token {
        writeln!(metadata, "Next page token: {token}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use lancedb::job::details::JobFragmentRow;
    use lancedb::job::details::{ChildHistoryAvailability, JobChildDetails, JobChildRow};
    #[test]
    fn discovery_preserves_ids_summaries_and_machine_output() {
        let response = ListJobChildrenResponse {
            job_id: "parent".into(),
            children: vec![JobChildRow {
                job_id: "child".into(),
                details: JobChildDetails::FunctionRefreshFragment(JobFragmentRow::unknown(42)),
            }],
            history_availability: ChildHistoryAvailability::Available,
            progress: None,
            progress_updated_at_millis: None,
            page_token: Some("next".into()),
        };
        for format in [
            OutputFormat::Json,
            OutputFormat::Table,
            OutputFormat::Csv,
            OutputFormat::NdJson,
        ] {
            let (mut output, mut diagnostics) = (Vec::new(), Vec::new());
            write_children(&response, format, &mut output, &mut diagnostics).unwrap();
            let output = String::from_utf8(output).unwrap();
            let diagnostics = String::from_utf8(diagnostics).unwrap();
            assert!(output.contains("child"));
            if matches!(format, OutputFormat::Json) {
                let value: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(value["children"][0]["summary"]["fragment_id"], 42);
                assert_eq!(value["page_token"], "next");
                assert!(diagnostics.is_empty());
            } else if matches!(format, OutputFormat::Table) {
                assert!(output.contains("Next page token: next"));
                assert!(diagnostics.is_empty());
            } else {
                assert!(!output.contains("History availability:"));
                assert!(diagnostics.contains("Next page token: next"));
            }
        }
    }
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        job: crate::job::JobArgs,
    }
    #[test]
    fn child_discovery_and_existing_events_accept_returned_identity() {
        assert!(Cli::try_parse_from([
            "test",
            "children",
            "--job-id",
            "parent",
            "--page-token",
            "next"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "test",
            "events",
            "--job-id",
            "child",
            "--order",
            "oldest-first",
            "--limit",
            "10"
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["test", "fragments", "list", "--job-id", "parent"]).is_err());
    }
}
