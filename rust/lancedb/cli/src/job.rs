// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use anyhow::{anyhow, Result};
use arrow_cast::pretty::pretty_format_batches;
use clap::{Args, Subcommand, ValueEnum};
use futures::TryStreamExt;
use lancedb::job::details::{
    job_event_page_token, JobEventOrder, JobListOptions as ListJobsRequest,
    JobListPage as ListJobsResponse, JobListRow as ListJobRow, TableRef,
};
use lancedb::job::{JobCancelOptions, JobEventOptions};
use std::io::Write;

use crate::formatter::OutputFormat;
use crate::server_config::ConnectionArgs;
use crate::table_writer::TableWriter;

#[derive(Args, Debug)]
pub struct JobArgs {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Describe a job.
    Describe(DescribeJobArgs),

    /// Cancel a job.
    Cancel(CancelJobArgs),

    /// List jobs.
    List(ListJobsArgs),

    /// Query event history for a job.
    Events(JobEventsArgs),

    /// List child diagnostic jobs and their progress.
    Children(crate::job_children::ChildrenArgs),
}

#[derive(Args, Debug)]
pub struct DescribeJobArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Job ID
    #[arg(long)]
    pub job_id: String,
}

#[derive(Args, Debug)]
pub struct CancelJobArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Job ID
    #[arg(long)]
    pub job_id: String,

    /// Human-readable reason to record on the canceled job event.
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Args, Debug)]
pub struct ListJobsArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Maximum number of jobs to return.
    #[arg(long)]
    pub limit: Option<u32>,

    /// Job type filter
    #[arg(long)]
    pub job_type: Option<String>,

    /// Database name for a table-scoped filter.
    #[arg(long)]
    pub db_name: Option<String>,

    /// Namespace for a table-scoped filter. Defaults to public when omitted.
    #[arg(long)]
    pub namespace: Option<String>,

    /// Table name for a table-scoped filter.
    #[arg(long)]
    pub table_name: Option<String>,

    /// Branch name for a table-scoped filter.
    #[arg(long)]
    pub branch_name: Option<String>,

    /// Job state filter
    #[arg(long)]
    pub state: Option<String>,

    /// Page token from a previous list request.
    #[arg(long)]
    pub page_token: Option<String>,
}

#[derive(Args, Debug)]
pub struct JobEventsArgs {
    #[command(flatten)]
    pub connection: ConnectionArgs,

    /// Job ID
    #[arg(long)]
    pub job_id: String,

    /// Maximum number of events to return. Without it the job's whole event
    /// history is returned. When it cuts the history short, a page token for
    /// the rest is printed.
    #[arg(long)]
    pub limit: Option<u32>,

    /// SQL-like event filter over state, updated_by, emitted_from, emitted_by, and claim_entity.
    #[arg(long)]
    pub filter: Option<String>,

    /// Which end of the history to start from. A --page-token carries the
    /// order it was issued for.
    #[arg(long, value_enum)]
    pub order: Option<JobEventOrderArg>,

    /// Continue from the page token printed by a previous --limit run.
    #[arg(long)]
    pub page_token: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum JobEventOrderArg {
    NewestFirst,
    OldestFirst,
}

impl From<JobEventOrderArg> for JobEventOrder {
    fn from(order: JobEventOrderArg) -> Self {
        match order {
            JobEventOrderArg::NewestFirst => Self::NewestFirst,
            JobEventOrderArg::OldestFirst => Self::OldestFirst,
        }
    }
}

pub async fn run_command(args: JobArgs, format: String) -> Result<()> {
    match args.command {
        Commands::Describe(args) => run_describe_command(args).await,
        Commands::Cancel(args) => run_cancel_command(args).await,
        Commands::List(args) => run_list_command(args, format.parse()?).await,
        Commands::Events(args) => run_events_command(args).await,
        Commands::Children(args) => crate::job_children::run_command(args, format.parse()?).await,
    }
}

pub async fn run_describe_command(args: DescribeJobArgs) -> Result<()> {
    let db = connection(args.connection).await?;
    print!("{}", db.job_handle(args.job_id)?.details().await?);
    Ok(())
}

pub async fn run_cancel_command(args: CancelJobArgs) -> Result<()> {
    let db = connection(args.connection).await?;
    if !db
        .cancel_job_with_options(
            &args.job_id,
            JobCancelOptions {
                reason: args.reason,
            },
        )
        .await?
    {
        anyhow::bail!("Job not found: {}", args.job_id);
    }
    print!("CancelJobResponse(job_id = {})", args.job_id);
    Ok(())
}

pub async fn run_list_command(args: ListJobsArgs, format: OutputFormat) -> Result<()> {
    let table_ref = table_ref_from_list_args(&args)?;
    let admin = connection(args.connection).await?;
    let response = admin
        .list_jobs_page(ListJobsRequest {
            limit: args.limit,
            job_type: args.job_type,
            table_ref,
            // The admin listing stays cluster-wide.
            database: None,
            state: args.state,
            page_token: args.page_token,
        })
        .await?;
    format_list_jobs_response_to_writer(&response, format, &mut std::io::stdout(), None)?;
    Ok(())
}

fn format_list_jobs_response_to_writer<W: Write>(
    response: &ListJobsResponse,
    format: OutputFormat,
    writer: &mut W,
    terminal_width: Option<usize>,
) -> Result<()> {
    match format {
        OutputFormat::Table => format_list_jobs_as_table(response, writer, terminal_width),
        OutputFormat::Csv => format_list_jobs_as_csv(&response.jobs, writer),
        OutputFormat::Json => {
            writeln!(writer, "{}", serde_json::to_string_pretty(response)?)?;
            Ok(())
        }
        OutputFormat::NdJson => format_list_jobs_as_ndjson(&response.jobs, writer),
    }
}

fn format_list_jobs_as_table<W: Write>(
    response: &ListJobsResponse,
    writer: &mut W,
    terminal_width: Option<usize>,
) -> Result<()> {
    if response.jobs.is_empty() {
        writeln!(writer, "No jobs found")?;
    } else {
        TableWriter::print_table(
            writer,
            &list_jobs_headers(),
            &list_jobs_rows(&response.jobs),
            terminal_width,
        )?;
        writeln!(
            writer,
            "({} job{})",
            response.jobs.len(),
            if response.jobs.len() == 1 { "" } else { "s" }
        )?;
    }
    if let Some(page_token) = &response.page_token {
        writeln!(writer, "Next page token: {page_token}")?;
    }
    Ok(())
}

fn format_list_jobs_as_csv<W: Write>(jobs: &[ListJobRow], writer: &mut W) -> Result<()> {
    writeln!(writer, "{}", list_jobs_headers().join(","))?;
    for row in list_jobs_rows(jobs) {
        let escaped = row
            .into_iter()
            .map(csv_escape)
            .collect::<Vec<_>>()
            .join(",");
        writeln!(writer, "{escaped}")?;
    }
    Ok(())
}

fn format_list_jobs_as_ndjson<W: Write>(jobs: &[ListJobRow], writer: &mut W) -> Result<()> {
    for job in jobs {
        let row = serde_json::json!({
            "table": job.table,
            "job_id": job.job_id,
            "job_type": job.job_type,
            "created_at": format_created_at(job.created_at_millis),
            "state": job.state,
            "paused": job.paused,
        });
        writeln!(writer, "{}", serde_json::to_string(&row)?)?;
    }
    Ok(())
}

fn list_jobs_headers() -> Vec<String> {
    // Trailing, so a reader that splits on the existing columns is unaffected.
    [
        "table",
        "job_id",
        "job_type",
        "created_at",
        "state",
        "paused",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

fn list_jobs_rows(jobs: &[ListJobRow]) -> Vec<Vec<String>> {
    jobs.iter()
        .map(|job| {
            vec![
                job.table.clone(),
                job.job_id.clone(),
                job.job_type.clone(),
                format_created_at(job.created_at_millis),
                job.state.clone(),
                job.paused.to_string(),
            ]
        })
        .collect()
}

fn format_created_at(created_at_millis: i64) -> String {
    milliseconds_since_epoch_to_str(created_at_millis)
}

fn csv_escape(value: String) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}

pub async fn run_events_command(args: JobEventsArgs) -> Result<()> {
    let db = connection(args.connection).await?;
    let job = db.job_handle(args.job_id)?;
    let mut pages = std::pin::pin!(job.event_pages(JobEventOptions {
        limit: args.limit,
        filter: args.filter,
        order: args.order.map(JobEventOrder::from),
        page_token: args.page_token,
    }));
    let mut printed_rows = false;
    let mut next_page_token = None;
    let mut history_availability = None;
    while let Some(page) = pages.try_next().await? {
        next_page_token = job_event_page_token(&page);
        history_availability = page.first().and_then(|batch| {
            batch
                .schema()
                .metadata()
                .get(lancedb::job::details::CHILD_AVAILABILITY_KEY)
                .cloned()
        });
        if page.iter().all(|batch| batch.num_rows() == 0) {
            continue;
        }
        printed_rows = true;
        println!("{}", pretty_format_batches(&page)?);
    }
    if !printed_rows {
        println!("No job events found");
    }
    if let Some(availability) = history_availability {
        println!("History availability: {availability}");
    }
    if let Some(page_token) = next_page_token {
        println!("Next page token: {page_token}");
    }
    Ok(())
}

fn table_ref_from_list_args(args: &ListJobsArgs) -> Result<Option<TableRef>> {
    match (&args.db_name, &args.table_name) {
        (Some(db_name), Some(table_name)) => {
            let namespace_path = args
                .namespace
                .as_ref()
                .map(|namespace| vec![namespace.clone()])
                .unwrap_or_default();
            TableRef::from_parts(
                db_name,
                namespace_path,
                table_name,
                args.branch_name.clone(),
                None,
            )
            .map(Some)
            .map_err(|err| anyhow!("invalid table filter: {err}"))
        }
        (None, None) => {
            if args.namespace.is_some() || args.branch_name.is_some() {
                return Err(anyhow!(
                    "--namespace and --branch-name require --db-name and --table-name"
                ));
            }
            Ok(None)
        }
        (Some(_), None) => Err(anyhow!("--table-name is required when using --db-name")),
        (None, Some(_)) => Err(anyhow!("--db-name is required when using --table-name")),
    }
}

pub(crate) async fn connection(connection: ConnectionArgs) -> Result<lancedb::Connection> {
    crate::connection::create_database_connection(&connection.resolve(None)?).await
}

fn milliseconds_since_epoch_to_str(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_args() -> ListJobsArgs {
        ListJobsArgs {
            connection: ConnectionArgs::default(),
            limit: None,
            job_type: None,
            db_name: None,
            namespace: None,
            table_name: None,
            branch_name: None,
            state: None,
            page_token: None,
        }
    }

    fn list_response() -> ListJobsResponse {
        ListJobsResponse {
            jobs: vec![
                ListJobRow {
                    table: "fineweb_edu".to_string(),
                    job_id: "j1-ip-0mt1y500f-pmkt17yz-demos:fineweb_edu".to_string(),
                    job_type: "reindex_ivf_pq".to_string(),
                    state: "in_progress".to_string(),
                    created_at_millis: 0,
                    paused: true,
                    ..Default::default()
                },
                ListJobRow {
                    table: "docs,archive".to_string(),
                    job_id: "job-2".to_string(),
                    job_type: "refresh".to_string(),
                    state: "failed".to_string(),
                    created_at_millis: 1_704_067_200_000,
                    ..Default::default()
                },
            ],
            page_token: Some("cursor-2".to_string()),
        }
    }

    fn format_list_jobs(format: OutputFormat) -> String {
        let mut output = Vec::new();
        format_list_jobs_response_to_writer(&list_response(), format, &mut output, Some(200))
            .unwrap();
        String::from_utf8(output).unwrap()
    }

    #[test]
    fn table_ref_from_list_args_uses_separate_table_inputs() {
        let args = ListJobsArgs {
            db_name: Some("analytics".to_string()),
            namespace: Some("prod".to_string()),
            table_name: Some("events".to_string()),
            branch_name: Some("dev".to_string()),
            ..list_args()
        };

        let table_ref = table_ref_from_list_args(&args).unwrap().unwrap();

        assert_eq!(
            table_ref.db_table_branch_id().unwrap(),
            "analytics:prod$events:dev"
        );
    }

    #[test]
    fn table_ref_from_list_args_requires_db_and_table_together() {
        let table_only = ListJobsArgs {
            table_name: Some("events".to_string()),
            ..list_args()
        };
        assert!(table_ref_from_list_args(&table_only).is_err());

        let db_only = ListJobsArgs {
            db_name: Some("analytics".to_string()),
            ..list_args()
        };
        assert!(table_ref_from_list_args(&db_only).is_err());

        let branch_only = ListJobsArgs {
            branch_name: Some("dev".to_string()),
            ..list_args()
        };
        assert!(table_ref_from_list_args(&branch_only).is_err());
    }

    #[test]
    fn formats_list_jobs_as_readable_table_with_page_token() {
        let output = format_list_jobs(OutputFormat::Table);

        assert!(output.contains("table"));
        assert!(output.contains("job_id"));
        assert!(output.contains("fineweb_edu"));
        assert!(output.contains("j1-ip-0mt1y500f-pmkt17yz-demos:fineweb_edu"));
        assert!(output.contains("created_at"));
        assert!(output.contains("1970-01-01 00:00:00"));
        assert!(output.contains("(2 jobs)"));
        assert!(output.contains("Next page token: cursor-2"));
        assert!(!output.contains("ListJobsResponse("));
    }

    #[test]
    fn formats_list_jobs_json_as_full_response_envelope() {
        let output = format_list_jobs(OutputFormat::Json);
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();

        assert_eq!(
            value["jobs"][0]["job_id"],
            "j1-ip-0mt1y500f-pmkt17yz-demos:fineweb_edu"
        );
        assert_eq!(value["jobs"][0]["created_at_millis"], 0);
        assert_eq!(value["page_token"], "cursor-2");
    }

    #[test]
    fn formats_list_jobs_csv_as_rows_without_page_token() {
        let output = format_list_jobs(OutputFormat::Csv);

        assert!(output.starts_with("table,job_id,job_type,created_at,state,paused\n"));
        assert!(output.contains("fineweb_edu,j1-ip-0mt1y500f-pmkt17yz-demos:fineweb_edu,reindex_ivf_pq,1970-01-01 00:00:00,in_progress,true"));
        assert!(output.contains("\"docs,archive\",job-2,refresh,2024-01-01 00:00:00,failed,false"));
        assert!(!output.contains("cursor-2"));
    }

    #[test]
    fn formats_list_jobs_ndjson_as_rows_without_page_token() {
        let output = format_list_jobs(OutputFormat::NdJson);
        let lines = output.lines().collect::<Vec<_>>();

        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["table"], "fineweb_edu");
        assert_eq!(first["created_at"], "1970-01-01 00:00:00");
        assert!(!output.contains("cursor-2"));
    }

    #[test]
    fn formats_empty_list_jobs_table() {
        let response = ListJobsResponse {
            jobs: Vec::new(),
            page_token: None,
        };
        let mut output = Vec::new();
        format_list_jobs_response_to_writer(&response, OutputFormat::Table, &mut output, Some(200))
            .unwrap();

        assert_eq!(String::from_utf8(output).unwrap(), "No jobs found\n");
    }
}
