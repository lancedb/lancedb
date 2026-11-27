// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::formatter::{OutputFormat, QueryResult, ResultFormatter};
use crate::server_config::{
    parse_endpoint, parse_grpc_endpoint, Authentication, ResolvedConnection, DEFAULT_HTTP_PORT,
    DEFAULT_SQL_PORT,
};
use crate::sql_executor::SdkSqlExecutor;
use crate::sql_executor::{SqlQueryExecutor, SqlResult};
use anyhow::{anyhow, Context, Result};
use arrow_schema::Schema;
use directories::ProjectDirs;
use futures::TryStreamExt;
use lancedb::listing::ListingOptions;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Editor, Helper};
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::tokenizer::{Location, Token, Tokenizer};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio::signal;

const LANCEDB_ASCII_ART: &str = r#" _                         ____  ____  
| |    __ _ _ __   ___ ___|  _ \| __ ) 
| |   / _` | '_ \ / __/ _ \ | | |  _ \ 
| |__| (_| | | | | (_|  __/ |_| | |_) |
|_____\__,_|_| |_|\___\___|____/|____/ 
                                       "#;

/// Information about a database table
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub schema: Arc<Schema>,
    pub row_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub num_indices: Option<u64>,
    pub num_fragments: Option<u64>,
}

/// Information about an index
#[derive(Debug, Clone, serde::Serialize)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub index_type: String,
    pub columns: Vec<String>,
}

/// Statistics about an index
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub num_indexed_rows: u64,
    pub num_unindexed_rows: u64,
    pub distance_type: Option<String>,
}

/// Result of executing a metacommand
enum MetaCommandResult {
    /// Command was handled normally, continue REPL loop
    Handled,
    /// Replace the current input line with new content (for \e)
    ReplaceInput(String),
}

/// The main client structure using the new architecture
pub struct LanceDBClient<S: SqlQueryExecutor> {
    editor: Editor<LanceDBCompleter, rustyline::history::DefaultHistory>,
    history_path: PathBuf,
    sql_executor: S,
    sql_connected: bool,
    connection: Option<Arc<lancedb::Connection>>,
    formatter: ResultFormatter,
    current_database: Option<String>,
    connection_info: Option<ResolvedConnection>,
    interactive_mode: bool,
    authentication: Authentication,
    last_query: Option<String>,
    /// The last re-runnable command (a SQL query or a `\d*` listing
    /// metacommand), used by `\watch`. Excludes `\watch` itself so it can't
    /// recurse, and one-shot commands like `\c`/`\e`/`\q`.
    last_command: Option<String>,
    profile_name: String,
}

#[derive(Clone)]
struct LanceDBCompleter {
    table_names: Arc<std::sync::Mutex<Vec<String>>>,
    index_names: Arc<std::sync::Mutex<Vec<String>>>,
    qualified_index_names: Arc<std::sync::Mutex<Vec<String>>>,
}

impl LanceDBCompleter {
    fn new() -> Self {
        Self {
            table_names: Arc::new(std::sync::Mutex::new(Vec::new())),
            index_names: Arc::new(std::sync::Mutex::new(Vec::new())),
            qualified_index_names: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn update_table_names(&mut self, tables: Vec<String>) {
        if let Ok(mut names) = self.table_names.lock() {
            *names = tables;
        }
    }

    fn update_index_names(&mut self, indices: Vec<String>, qualified: Vec<String>) {
        if let Ok(mut names) = self.index_names.lock() {
            *names = indices;
        }
        if let Ok(mut names) = self.qualified_index_names.lock() {
            *names = qualified;
        }
    }
}

impl Completer for LanceDBCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let mut candidates = Vec::new();
        let trimmed = line[..pos].trim_start();

        // For \di commands, only complete index names (including qualified table.index)
        if trimmed.starts_with("\\di+ ") || trimmed.starts_with("\\di ") {
            let word_start = line[..pos]
                .rfind(|c: char| c.is_whitespace())
                .map(|i| i + 1)
                .unwrap_or(0);
            let partial = &line[word_start..pos];
            let partial_lower = partial.to_lowercase();

            // Offer both unqualified and qualified index names
            if let Ok(indices) = self.index_names.lock() {
                for index in indices.iter() {
                    if index.to_lowercase().starts_with(&partial_lower) {
                        candidates.push(Pair {
                            display: index.clone(),
                            replacement: index.clone(),
                        });
                    }
                }
            }
            if let Ok(qualified) = self.qualified_index_names.lock() {
                for name in qualified.iter() {
                    if name.to_lowercase().starts_with(&partial_lower) {
                        candidates.push(Pair {
                            display: name.clone(),
                            replacement: name.clone(),
                        });
                    }
                }
            }
            return Ok((word_start, candidates));
        }

        // SQL keywords
        let keywords = vec![
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "DELETE", "CREATE",
            "TABLE", "DROP", "ALTER", "INDEX", "ON", "SHOW", "TABLES", "DESCRIBE", "DESC", "LIMIT",
            "ORDER", "BY", "GROUP", "HAVING", "AS", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER",
            "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE",
            "END",
        ];

        // Find the word being completed
        let word_start = line[..pos]
            .rfind(|c: char| c.is_whitespace() || "(),;".contains(c))
            .map(|i| i + 1)
            .unwrap_or(0);

        let partial = &line[word_start..pos];
        let partial_upper = partial.to_uppercase();

        // Add SQL keywords
        for keyword in keywords {
            if keyword.starts_with(&partial_upper) {
                candidates.push(Pair {
                    display: keyword.to_string(),
                    replacement: keyword.to_string(),
                });
            }
        }

        // Add table names
        if let Ok(tables) = self.table_names.lock() {
            for table in tables.iter() {
                if table.to_lowercase().starts_with(&partial.to_lowercase()) {
                    candidates.push(Pair {
                        display: table.clone(),
                        replacement: table.clone(),
                    });
                }
            }
        }

        // Add index names
        if let Ok(indices) = self.index_names.lock() {
            for index in indices.iter() {
                if index.to_lowercase().starts_with(&partial.to_lowercase()) {
                    candidates.push(Pair {
                        display: index.clone(),
                        replacement: index.clone(),
                    });
                }
            }
        }

        Ok((word_start, candidates))
    }
}

impl Hinter for LanceDBCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for LanceDBCompleter {}
impl Validator for LanceDBCompleter {}
impl Helper for LanceDBCompleter {}

/// Unwrap one of a listing's per-table requests, reporting failure instead of
/// degrading silently.
///
/// The `\d`-family listings issue several requests per table and render a blank
/// cell for each one that fails, which reads as "this table has no stats"
/// rather than "this request failed". The error carries the request ID the
/// query node logged, so this warning is what makes such a failure traceable.
fn or_warn<T, E: std::fmt::Display>(result: std::result::Result<T, E>, subject: &str) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(e) => {
            eprintln!("warning: {subject} failed: {e}");
            None
        }
    }
}

/// Point `executor` at the Flight SQL address `sql_addr`, preserving its
/// scheme so an `https://`/`grpcs://` address dials TLS. Returns the URL that
/// was dialed along with any connection error, so the caller can report both
/// endpoint failures together.
///
/// Split out of `connect_internal` so the address-to-transport handoff is
/// reachable without a live REST endpoint.
async fn connect_sql_endpoint<S: SqlQueryExecutor + ?Sized>(
    executor: &mut S,
    sql_addr: &str,
    database: &str,
) -> Result<(String, Option<anyhow::Error>)> {
    let url = parse_grpc_endpoint(sql_addr, DEFAULT_SQL_PORT)?.url();
    executor.set_database(database.to_string());
    let err = executor.connect(&url).await.err();
    Ok((url, err))
}

fn connection_failure_message(endpoint: &str, protocol: &str, error: &anyhow::Error) -> String {
    // Include the source chain: transport errors often hide the useful cause
    // (connection refused, DNS, TLS, etc.) behind a generic outer message.
    format!("lancedb: connection failed: {endpoint} ({protocol}): {error:#}")
}

/// HTTP failures are fatal; SQL-only failures allow metadata commands.
fn report_connection_failures(
    sql: Option<(&str, &anyhow::Error)>,
    http: Option<(&str, &anyhow::Error)>,
) {
    for (protocol, failure) in [("sql", sql), ("http", http)] {
        if let Some((endpoint, error)) = failure {
            eprintln!("{}", connection_failure_message(endpoint, protocol, error));
        }
    }

    if sql.is_none() && http.is_none() {
        return;
    }

    if http.is_some() {
        std::process::exit(1);
    }

    eprintln!("lancedb: SQL queries will not work, but metadata commands are available");
}

// Helper function to run async operations with Ctrl+C cancellation
async fn run_with_cancellation<F, T>(operation: F) -> Result<Option<T>>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::select! {
        result = operation => {
            match result {
                Ok(value) => Ok(Some(value)),
                Err(e) => Err(e),
            }
        }
        _ = signal::ctrl_c() => {
            println!("\n^C");
            Ok(None)
        }
    }
}

/// Default interval for `\watch` when none is given (matches psql).
const DEFAULT_WATCH_INTERVAL_SECS: f64 = 2.0;

/// Smallest accepted `\watch` interval. A positive-but-tiny value (e.g.
/// `1e-20`, or `1e-6` which converts to a nonzero 1us) passes a bare `> 0.0`
/// check yet leaves effectively no delay between runs, spinning the loop and
/// hammering the server; anything below this floor is rejected.
const MIN_WATCH_INTERVAL_SECS: f64 = 0.1;

/// Whether a metacommand is something `\watch` should repeat. The `\d*`
/// listing/describe family (`\d`, `\di`, `\dt`, ...) qualifies; `\watch`
/// itself and one-shot commands (`\c`, `\e`, `\q`, `\?`) do not.
fn is_watchable_metacommand(cmd: &str) -> bool {
    // Exact-match the recognized `\d`-family verbs, not just a `\d` prefix:
    // an unrecognized `\d`-prefixed typo (`\dfoo`) is rejected by
    // `handle_metacommand` yet would otherwise clobber `last_command`.
    // Only the first token matters, so `\d TABLE` / `\di INDEX` still match.
    matches!(
        cmd.split_whitespace().next(),
        Some("\\d" | "\\d+" | "\\dt" | "\\dt+" | "\\di" | "\\di+" | "\\dj" | "\\dj+")
    )
}

/// The `SHOW JOBS` statement behind `\dj [TABLE]`. The argument is a literal
/// table name, so it is quoted: a name need not be a bare SQL identifier.
fn show_jobs_statement(table: Option<&str>) -> String {
    match table {
        Some(table) => format!("SHOW JOBS FOR \"{}\"", table.replace('"', "\"\"")),
        None => "SHOW JOBS".to_string(),
    }
}

/// Validate a `\watch` interval in seconds to a `Duration`, or `None` if it is
/// unusable. `try_from_secs_f64` rejects negative, NaN, non-finite ("inf"), and
/// `Duration`-overflowing ("1e300") inputs (so the sleep can't panic); the floor
/// then rejects zero and positive-but-tiny values that would leave no real delay
/// between runs. See [`MIN_WATCH_INTERVAL_SECS`].
fn watch_interval(secs: f64) -> Option<Duration> {
    let min = Duration::from_secs_f64(MIN_WATCH_INTERVAL_SECS);
    Duration::try_from_secs_f64(secs).ok().filter(|d| *d >= min)
}

/// Parsed `\watch` arguments: how often to repeat, and what to repeat.
struct WatchArgs {
    interval: Duration,
    /// Inline statement/listing to watch, or `None` to re-run the last command.
    target: Option<String>,
}

/// Parse the argument string after `\watch` into an interval and an optional
/// inline target. A first token that parses as a number is always treated as
/// the interval (real SQL never starts with a bare number), so it is validated
/// as one; anything else means the whole string is the inline target. Thus
/// `\watch` / `\watch 5` re-run the last command, while `\watch SELECT ...` and
/// `\watch 5 SELECT ...` watch that statement (at the default / given interval).
fn parse_watch_args(args: &str) -> Result<WatchArgs, String> {
    let args = args.trim();
    let default = Duration::from_secs_f64(DEFAULT_WATCH_INTERVAL_SECS);
    if args.is_empty() {
        return Ok(WatchArgs {
            interval: default,
            target: None,
        });
    }

    let mut it = args.splitn(2, char::is_whitespace);
    let first = it.next().unwrap_or("");
    let rest = it.next().unwrap_or("").trim();

    if let Ok(secs) = first.parse::<f64>() {
        let interval = watch_interval(secs).ok_or_else(|| {
            format!(
                "Invalid interval '{first}' (must be a number >= {MIN_WATCH_INTERVAL_SECS}s). \
                 Usage: \\watch [seconds] [statement]"
            )
        })?;
        return Ok(WatchArgs {
            interval,
            target: (!rest.is_empty()).then(|| rest.to_string()),
        });
    }

    Ok(WatchArgs {
        interval: default,
        target: Some(args.to_string()),
    })
}

/// Sets `NO_PAGER` for as long as it is alive, restoring the prior value on
/// drop. `should_use_pager` already honours `NO_PAGER`, so this disables
/// paging during a `\watch` loop. Restored even when the loop's future is
/// dropped by Ctrl-C, because the guard is a live local at the await point.
struct NoPagerGuard {
    prev: Option<String>,
}

impl NoPagerGuard {
    fn new() -> Self {
        let prev = std::env::var("NO_PAGER").ok();
        std::env::set_var("NO_PAGER", "1");
        Self { prev }
    }
}

impl Drop for NoPagerGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => std::env::set_var("NO_PAGER", v),
            None => std::env::remove_var("NO_PAGER"),
        }
    }
}

/// Check if output should use a pager
fn should_use_pager(output: &str) -> bool {
    if std::env::var("NO_PAGER").is_ok() {
        return false;
    }

    let term_height = termsize::get().map(|size| size.rows as usize).unwrap_or(24);
    let line_count = output.lines().count();

    line_count > (term_height * 80 / 100)
}

/// Find an appropriate pager
fn find_pager() -> Option<String> {
    if let Ok(pager) = std::env::var("PAGER") {
        if !pager.is_empty() {
            return Some(pager);
        }
    }

    let pagers = ["less", "more", "bat"];
    for pager in &pagers {
        if which::which(pager).is_ok() {
            return Some(pager.to_string());
        }
    }

    None
}

/// Leading whitespace and comments, which the server's tokenizer discards
/// before matching a statement's keywords.
fn strip_leading_trivia(query: &str) -> &str {
    let mut rest = query.trim_start();
    loop {
        let remainder = if let Some(after) = rest.strip_prefix("--") {
            after.find('\n').map_or("", |end| &after[end + 1..])
        } else if let Some(after) = rest.strip_prefix("/*") {
            after.find("*/").map_or("", |end| &after[end + 2..])
        } else {
            return rest;
        };
        rest = remainder.trim_start();
    }
}

/// Rows of a `SHOW JOBS` listing the interactive shell will print. The registry
/// can hold far more jobs than anyone reads at a terminal, and the server
/// returns the listing in one shot with no cursor to resume from, so the table
/// rendering cuts the display and points at ways to narrow the listing or
/// export every row.
const MAX_DISPLAYED_JOB_ROWS: usize = 1000;

/// Ready a `SHOW JOBS` listing for display, returning it with any notice to
/// print after the formatted output.
///
/// The `job_id` marking applies everywhere: it is the input to `CANCEL JOB`,
/// so it has to be copyable rather than readable. The row cut applies only to
/// the interactive table rendering -- CSV/JSON/NDJSON and one-shot invocations
/// are read by programs, which need every row and nothing but the payload.
fn prepare_show_jobs_result(
    result: SqlResult,
    interactive_table: bool,
) -> (SqlResult, Option<String>) {
    let total = crate::formatter::row_count(&result);
    let cut = interactive_table && total > MAX_DISPLAYED_JOB_ROWS;

    let notice = cut.then(|| {
        format!(
            "\nShowing the first {MAX_DISPLAYED_JOB_ROWS} of {total} jobs. Narrow the \
             listing with SHOW JOBS FOR <table>, or use lancedb sql --command 'SHOW JOBS' \
             --format json to retrieve all rows.\n"
        )
    });
    let result = if cut {
        crate::formatter::take_rows(result, MAX_DISPLAYED_JOB_ROWS)
    } else {
        result
    };
    (
        crate::formatter::mark_no_truncate(result, &["job_id"]),
        notice,
    )
}

/// The first two keywords of `query`, matched on leading keywords rather than
/// a raw prefix so that `SHOW  JOBS` and a comment-prefixed query both match
/// while `SHOW JOBSX` does not.
fn leading_keywords(query: &str) -> Option<(String, String)> {
    let mut words = strip_leading_trivia(query)
        .trim_end_matches(|c: char| c.is_whitespace() || c == ';')
        .split_whitespace();
    let first = words.next()?.to_ascii_uppercase();
    let second = words.next()?.to_ascii_uppercase();
    Some((first, second))
}

/// Whether `query` is a `SHOW JOBS` statement.
fn is_show_jobs(query: &str) -> bool {
    leading_keywords(query).is_some_and(|(first, second)| first == "SHOW" && second == "JOBS")
}

/// Columns of a statement's result that a reader copies rather than reads
/// (identifiers), which the table view must not abbreviate.
fn copyable_columns(query: &str) -> &'static [&'static str] {
    match leading_keywords(query) {
        Some((first, second)) if first == "CANCEL" && second == "JOB" => &["job_id"],
        // A Function version is the argument of the next statement a reader
        // writes, so it has to survive display in full.
        Some((first, second)) if first == "SHOW" && second == "FUNCTIONS" => &["version"],
        Some((first, second)) if first == "SHOW" && second == "ERRORS" => {
            &["job_id", "function_version"]
        }
        Some((first, second))
            if first == "REFRESH" && (second == "COLUMN" || second == "MATERIALIZED") =>
        {
            &["job_id"]
        }
        _ => &[],
    }
}

/// Display output with pager if needed
fn display_with_pager(output: &str, interactive_mode: bool) -> Result<()> {
    if !interactive_mode || !should_use_pager(output) {
        // Just print directly
        print!("{}", output);
        io::stdout().flush()?;
        return Ok(());
    }

    // Find pager
    let Some(pager_cmd) = find_pager() else {
        // No pager available, just print
        print!("{}", output);
        io::stdout().flush()?;
        return Ok(());
    };

    // Set up pager process
    let mut cmd = Command::new(&pager_cmd);

    // If using less, add -FRX flags:
    // -F: quit if one screen
    // -R: raw control chars (for colors)
    // -X: no init (don't clear screen)
    if pager_cmd.contains("less") {
        cmd.arg("-FRX");
    }

    cmd.stdin(Stdio::piped())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // Spawn pager
    let mut child = cmd.spawn()?;

    // Write output to pager
    if let Some(mut stdin) = child.stdin.take() {
        // Ignore SIGPIPE errors when pager exits early (e.g., user presses 'q')
        let _ = stdin.write_all(output.as_bytes());
        let _ = stdin.flush();
    }

    // Wait for pager to finish
    let _ = child.wait();

    Ok(())
}

impl<S: SqlQueryExecutor> LanceDBClient<S>
where
    S: SqlQueryExecutor,
{
    fn prompt(&self) -> String {
        match &self.current_database {
            Some(db) => format!("[{}]:{}=# ", self.profile_name, db),
            None => format!("[{}]:lancedb=# ", self.profile_name),
        }
    }

    fn continuation_prompt(&self) -> &'static str {
        "         # "
    }

    fn open_editor_for_last_query(&self) -> Result<Option<String>> {
        // Use last executed query if available, otherwise empty string
        let initial_query = self.last_query.as_deref().unwrap_or("");
        self.open_editor_for_query(initial_query)
    }

    fn display_query_lines(&self, query: &str) {
        // Display each line of the query as if the user had typed it
        for (i, line) in query.lines().enumerate() {
            if i == 0 {
                println!("{}{}", self.prompt(), line);
            } else {
                println!("{}{}", self.continuation_prompt(), line);
            }
        }
    }

    fn open_editor_for_query(&self, initial_query: &str) -> Result<Option<String>> {
        // Determine which editor to use
        let editor = std::env::var("EDITOR")
            .or_else(|_| std::env::var("VISUAL"))
            .unwrap_or_else(|_| {
                for editor in &["vim", "vi", "emacs", "nano", "code"] {
                    if which::which(editor).is_ok() {
                        return editor.to_string();
                    }
                }
                "vi".to_string()
            });

        // Create a temporary file with .sql extension
        let mut temp_file = NamedTempFile::with_suffix(".sql")
            .map_err(|e| anyhow!("Failed to create temporary file: {}", e))?;

        // Write initial query to the file
        temp_file
            .write_all(initial_query.as_bytes())
            .map_err(|e| anyhow!("Failed to write to temporary file: {}", e))?;
        temp_file
            .flush()
            .map_err(|e| anyhow!("Failed to flush temporary file: {}", e))?;

        // Get the path before we move ownership
        let temp_path = temp_file.path().to_path_buf();

        // Launch the editor
        let status = Command::new(&editor)
            .arg(&temp_path)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .status()
            .map_err(|e| anyhow!("Failed to launch editor '{}': {}", editor, e))?;

        if !status.success() {
            // Editor exited with error, but we'll still try to read the file
            eprintln!("Editor exited with non-zero status");
        }

        // Read the edited content
        let edited_content = std::fs::read_to_string(&temp_path)
            .map_err(|e| anyhow!("Failed to read edited file: {}", e))?;

        // Check if the content is empty or unchanged
        let trimmed = edited_content.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        Ok(Some(edited_content))
    }

    pub async fn new(
        database_override: Option<String>,
        interactive_mode: bool,
        output_format: OutputFormat,
        authentication: Authentication,
        profile_name: String,
        sql_executor: S,
    ) -> Result<Self> {
        // Set up history file
        let history_path = if let Some(proj_dirs) = ProjectDirs::from("com", "lancedb", "client") {
            let data_dir = proj_dirs.data_dir();
            std::fs::create_dir_all(data_dir)?;
            data_dir.join("history.txt")
        } else {
            PathBuf::from("/tmp/lancedb-history.txt")
        };

        // Create completer
        let completer = LanceDBCompleter::new();

        // Create editor with completer
        let mut editor = Editor::with_config(
            rustyline::config::Config::builder()
                .completion_type(rustyline::config::CompletionType::List)
                .build(),
        )
        .map_err(|e| anyhow!("Failed to create editor: {}", e))?;
        editor.set_helper(Some(completer));
        let _ = editor.load_history(&history_path);

        // Use database override, or system username as default database
        let default_database = database_override.unwrap_or_else(|| {
            std::env::var("USER")
                .or_else(|_| std::env::var("USERNAME"))
                .unwrap_or_else(|_| "test".to_string())
        });

        let formatter = ResultFormatter::new(output_format);

        Ok(Self {
            editor,
            history_path,
            sql_executor,
            sql_connected: false,
            connection: None,
            formatter,
            current_database: Some(default_database),
            connection_info: None,
            interactive_mode,
            authentication,
            last_query: None,
            last_command: None,
            profile_name,
        })
    }

    pub async fn connect(&mut self, conn: &ResolvedConnection) -> Result<()> {
        self.connect_internal(conn, true).await
    }

    async fn connect_internal(
        &mut self,
        conn: &ResolvedConnection,
        show_message: bool,
    ) -> Result<()> {
        let sql_addr = &conn.sql_server_url;
        let http_addr = &conn.http_server_url;

        // The REST address keeps whichever scheme the user configured, so an
        // https:// address stays on TLS all the way to the transport.
        let lancedb_uri = parse_endpoint(http_addr, DEFAULT_HTTP_PORT)?.url();

        // Build the database URI
        let db_name = self.current_database.as_deref().unwrap_or("test");

        // Try both connections and report failures together
        let (flight_server, sql_err) =
            connect_sql_endpoint(&mut self.sql_executor, sql_addr, db_name).await?;

        let db_uri = format!("db://{}", db_name);

        // lancedb::connect() is lazy, so we create the connection then probe it
        // with a table_names call to verify the HTTP endpoint is reachable.
        let mut builder = lancedb::connect(&db_uri)
            .region("self-hosted")
            .host_override(&lancedb_uri);
        if let Authentication::ApiKey(api_key) = &self.authentication {
            builder = builder.api_key(api_key);
        } else {
            // The SDK requires an explicit API key even with a header provider.
            // An empty key omits x-api-key; client_config supplies the OIDC headers.
            builder = builder.api_key("");
        }
        if let Some(client_config) = crate::lancedb_client_config(&conn.tls, &self.authentication) {
            builder = builder.client_config(client_config);
        }
        let connection = builder
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to connect to LanceDB: {}", e))?;

        let http_err = connection
            .list_tables(&[], ListingOptions::default().page_limit(1))
            .next_page()
            .await
            .err()
            .map(anyhow::Error::from);

        report_connection_failures(
            sql_err.as_ref().map(|err| (flight_server.as_str(), err)),
            http_err.as_ref().map(|err| (lancedb_uri.as_str(), err)),
        );

        self.connection = Some(Arc::new(connection));
        self.sql_connected = sql_err.is_none();
        self.connection_info = Some(conn.clone());

        // Only print connection message in interactive mode and when requested
        if self.interactive_mode && show_message {
            println!("Connected to SQL: {}, HTTP: {}", sql_addr, http_addr);
        }

        // Update table name completions
        self.update_completions().await;

        Ok(())
    }

    /// Fetch all table names with pagination
    async fn table_names(connection: &lancedb::Connection) -> Result<Vec<String>> {
        let mut all_tables: Vec<String> = connection
            .list_tables(&[], ListingOptions::default().page_limit(100))
            .try_collect()
            .await
            .map_err(|e| anyhow!("Failed to list tables: {}", e))?;

        // The listing resumes from the store's own cursor, so its pages arrive in key order
        // rather than name order. Callers here -- `\d`, `\dt`, and completion -- present the
        // whole list to a person, and this walk already holds it in memory, so sort it once.
        all_tables.sort();

        Ok(all_tables)
    }

    async fn update_completions(&mut self) {
        if let Some(connection) = &self.connection {
            if let Ok(table_names) = Self::table_names(connection).await {
                if let Some(helper) = self.editor.helper_mut() {
                    helper.update_table_names(table_names.clone());

                    // Also gather all index names (unqualified and qualified)
                    let mut all_index_names = Vec::new();
                    let mut qualified_index_names = Vec::new();
                    for table_name in &table_names {
                        if let Ok(table) = connection.open_table(table_name).execute().await {
                            if let Ok(indices) = table.list_indices().await {
                                for index in indices {
                                    qualified_index_names
                                        .push(format!("{}.{}", table_name, index.name));
                                    all_index_names.push(index.name);
                                }
                            }
                        }
                    }
                    helper.update_index_names(all_index_names, qualified_index_names);
                }
            }
        }
    }

    pub async fn run_repl(&mut self) -> Result<()> {
        println!("{}", LANCEDB_ASCII_ART);
        println!("LanceDB CLI v{}", crate::build_version());
        println!("Type \\? for help or \\q to quit\n");

        let mut multiline_query = String::new();
        let mut in_multiline = false;

        loop {
            let prompt_string;
            let prompt = if in_multiline {
                self.continuation_prompt()
            } else {
                prompt_string = self.prompt();
                &prompt_string
            };

            match self.editor.readline(prompt) {
                Ok(mut line) => {
                    let trimmed = line.trim();

                    // Skip empty lines
                    if trimmed.is_empty() {
                        continue;
                    }

                    // Handle metacommands
                    if !in_multiline && trimmed.starts_with('\\') {
                        match run_with_cancellation(self.handle_metacommand(trimmed)).await {
                            Err(e) => {
                                eprintln!("ERROR: {}", e);
                                continue;
                            }
                            Ok(None) => {
                                // Cancelled
                                continue;
                            }
                            Ok(Some(result)) => {
                                match result {
                                    MetaCommandResult::Handled => {
                                        // Successful metacommand
                                        self.editor.add_history_entry(&line)?;
                                        // Remember listing metacommands so
                                        // \watch can repeat them.
                                        if is_watchable_metacommand(trimmed) {
                                            self.last_command = Some(trimmed.to_string());
                                        }
                                        continue;
                                    }
                                    MetaCommandResult::ReplaceInput(new_input) => {
                                        line = new_input;
                                        self.display_query_lines(&line);
                                        // Fall through to process the new input
                                    }
                                }
                            }
                        }
                    }

                    // Now process line normally (either from readline or editor)
                    let trimmed = line.trim();

                    // Skip empty lines (in case editor returned empty)
                    if trimmed.is_empty() {
                        continue;
                    }

                    // Add to history
                    self.editor.add_history_entry(&line)?;

                    // Normal query accumulation
                    if in_multiline {
                        multiline_query.push('\n');
                    }
                    multiline_query.push_str(&line);

                    // Check if query is complete (ends with semicolon)
                    if multiline_query.trim().ends_with(';') {
                        // Store the complete query with semicolon before removing it
                        self.last_query = Some(multiline_query.clone());

                        // Remove the semicolon
                        multiline_query.pop();
                        // Remember the query so \watch can repeat it.
                        self.last_command = Some(multiline_query.clone());

                        // Execute the query
                        match run_with_cancellation(self.execute_query(&multiline_query)).await {
                            Ok(Some(())) => {}
                            Ok(None) => {}
                            Err(e) => {
                                eprintln!("ERROR: {}", e);
                            }
                        }

                        // Reset for next query
                        multiline_query.clear();
                        in_multiline = false;
                    } else {
                        in_multiline = true;
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    multiline_query.clear();
                    in_multiline = false;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\q");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }

        // Save history
        self.editor.save_history(&self.history_path)?;
        Ok(())
    }

    async fn execute_query(&mut self, query: &str) -> Result<()> {
        let start_time = Instant::now();

        // Check if this is a DDL statement and get the command type
        let ddl_command = {
            let trimmed = query.trim().to_uppercase();
            if trimmed.starts_with("CREATE TABLE") {
                Some("CREATE TABLE")
            } else if trimmed.starts_with("CREATE INDEX") {
                Some("CREATE INDEX")
            } else if trimmed.starts_with("CREATE VIEW") {
                Some("CREATE VIEW")
            } else if trimmed.starts_with("CREATE") {
                Some("CREATE")
            } else if trimmed.starts_with("ALTER TABLE") {
                Some("ALTER TABLE")
            } else if trimmed.starts_with("ALTER INDEX") {
                Some("ALTER INDEX")
            } else if trimmed.starts_with("ALTER VIEW") {
                Some("ALTER VIEW")
            } else if trimmed.starts_with("ALTER") {
                Some("ALTER")
            } else if trimmed.starts_with("DROP TABLE") {
                Some("DROP TABLE")
            } else if trimmed.starts_with("DROP INDEX") {
                Some("DROP INDEX")
            } else if trimmed.starts_with("DROP VIEW") {
                Some("DROP VIEW")
            } else if trimmed.starts_with("DROP") {
                Some("DROP")
            } else {
                None
            }
        };

        if !self.sql_connected {
            return Err(anyhow!("SQL endpoint is not connected"));
        }

        // Execute the query
        let result = self.sql_executor.execute_query(query).await?;

        // Display output based on whether it's DDL or not
        if let Some(command) = ddl_command {
            // For DDL, just echo the command type
            println!("{}", command);
        } else {
            // For non-DDL, format and display the result.
            let (result, notice) = if is_show_jobs(query) {
                let interactive_table = self.interactive_mode
                    && matches!(self.formatter.output_format(), OutputFormat::Table);
                prepare_show_jobs_result(result, interactive_table)
            } else {
                (
                    crate::formatter::mark_no_truncate(result, copyable_columns(query)),
                    None,
                )
            };
            let query_result = QueryResult::SqlResult(result);
            let mut output = self.formatter.format(&query_result)?;
            // Appended rather than printed ahead of the payload, so the notice
            // travels through the pager with the table it describes.
            if let Some(notice) = notice {
                output.push_str(&notice);
            }
            display_with_pager(&output, self.interactive_mode)?;
        }

        let elapsed = start_time.elapsed();
        if self.interactive_mode {
            println!("Time: {:.3} ms", elapsed.as_secs_f64() * 1000.0);
        }

        Ok(())
    }

    async fn handle_metacommand(&mut self, command: &str) -> Result<MetaCommandResult> {
        let parts: Vec<&str> = command.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(MetaCommandResult::Handled);
        }

        let cmd = parts[0];
        match cmd {
            "\\?" | "\\h" | "\\help" | "help" => {
                self.print_help();
            }
            "\\d" | "\\d+" => {
                let detailed = cmd.ends_with('+');
                if parts.len() < 2 {
                    // \d without argument lists tables
                    self.list_tables(detailed).await?;
                } else {
                    // \d <table> describes the table
                    self.describe_table(parts[1]).await?;
                }
            }
            "\\di" | "\\di+" => {
                let detailed = cmd.ends_with('+');
                if parts.len() < 2 {
                    // \di without argument lists all indexes
                    self.list_all_indexes(detailed).await?;
                } else {
                    // \di <index> describes the specific index
                    self.describe_index(parts[1], detailed).await?;
                }
            }
            "\\dt" | "\\dt+" => {
                let detailed = cmd.ends_with('+');
                self.list_tables(detailed).await?;
            }
            // `\dj+` is a synonym for now.
            "\\dj" | "\\dj+" => {
                let statement = show_jobs_statement(parts.get(1).copied());
                self.execute_query(&statement).await?;
            }
            "\\watch" => {
                let args = command.strip_prefix("\\watch").unwrap_or("");
                let spec = match parse_watch_args(args) {
                    Ok(spec) => spec,
                    Err(msg) => {
                        eprintln!("{msg}");
                        return Ok(MetaCommandResult::Handled);
                    }
                };

                // Resolve what to repeat: an inline statement/listing if given,
                // otherwise the last command. An inline `\`-target must be a
                // repeatable `\d`-family listing -- this also rejects `\watch
                // \watch ...`, which would otherwise recurse.
                let target = match spec.target {
                    Some(t) if t.starts_with('\\') => {
                        if !is_watchable_metacommand(&t) {
                            eprintln!(
                                "\\watch: cannot watch '{t}'. Watch a SQL query or a \\d-family listing."
                            );
                            return Ok(MetaCommandResult::Handled);
                        }
                        t
                    }
                    // Drop a trailing ';' so inline SQL matches the re-run form.
                    Some(t) => t.trim_end_matches(';').trim_end().to_string(),
                    None => {
                        let Some(last) = self.last_command.clone() else {
                            eprintln!("\\watch: no previous query or listing to watch.");
                            return Ok(MetaCommandResult::Handled);
                        };
                        last
                    }
                };
                self.watch_loop(target, spec.interval).await?;
            }
            "\\c" => {
                if parts.len() < 2 {
                    match &self.current_database {
                        Some(db) => println!("You are now connected to database \"{}\".", db),
                        None => println!("You are not connected to a database."),
                    }
                } else {
                    let new_db = parts[1].to_string();

                    // Check if we're already connected to this database
                    if let Some(current_db) = &self.current_database {
                        if current_db == &new_db {
                            println!("You are now connected to database \"{}\".", new_db);
                            return Ok(MetaCommandResult::Handled);
                        }
                    }

                    let old_db = self.current_database.clone();
                    self.current_database = Some(new_db.clone());

                    // If we're connected, reconnect with the new database
                    if let Some(conn) = &self.connection_info.clone() {
                        match self.connect_internal(conn, false).await {
                            Ok(_) => {
                                println!("You are now connected to database \"{}\".", new_db);
                            }
                            Err(e) => {
                                eprintln!("connection to database \"{}\" failed: {}", new_db, e);
                                // Restore the previous database on failure
                                self.current_database = old_db;
                            }
                        }
                    } else {
                        println!("You are now connected to database \"{}\".", new_db);
                    }
                }
            }
            "\\e" => {
                // Open editor and return the edited query
                match self.open_editor_for_last_query() {
                    Ok(Some(edited)) => {
                        return Ok(MetaCommandResult::ReplaceInput(edited.trim().to_string()));
                    }
                    Ok(None) => {
                        // User cancelled or cleared editor
                        return Ok(MetaCommandResult::Handled);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            "\\q" | "\\quit" => {
                println!("Use Ctrl+D to exit");
            }
            _ => {
                eprintln!("Invalid command \\{}. Try \\? for help.", &cmd[1..]);
            }
        }
        Ok(MetaCommandResult::Handled)
    }

    /// Re-run `command` (a SQL query or a `\d*` listing) every `interval`
    /// seconds, clearing the screen between runs, until interrupted with
    /// Ctrl-C. The REPL wraps this in `run_with_cancellation`, so Ctrl-C
    /// drops this future and returns to the prompt; `NoPagerGuard` restores
    /// the pager setting on that drop. Paging is suppressed for the duration
    /// so a long listing doesn't block the loop waiting on the pager.
    async fn watch_loop(&mut self, command: String, interval: Duration) -> Result<()> {
        let _no_pager = NoPagerGuard::new();
        loop {
            // Clear screen, cursor home, then a header line.
            print!("\x1b[2J\x1b[H");
            println!(
                "Watch every {:.1}s -- {}    (Ctrl-C to stop)\n",
                interval.as_secs_f64(),
                command
            );
            let _ = io::stdout().flush();

            let result = if command.starts_with('\\') {
                // Recursion (handle_metacommand -> watch_loop ->
                // handle_metacommand) needs boxing to keep the future sized.
                Box::pin(self.handle_metacommand(&command))
                    .await
                    .map(|_| ())
            } else {
                Box::pin(self.execute_query(&command)).await
            };
            if let Err(e) = result {
                eprintln!("ERROR: {}", e);
            }
            let _ = io::stdout().flush();

            tokio::time::sleep(interval).await;
        }
    }

    fn print_help(&self) {
        println!("\nGeneral");
        println!("  \\h, \\?, help          show this help");
        println!("  \\q                     quit lancedb (or use Ctrl+D)");
        println!("\nEditing");
        println!("  \\e                     edit query buffer in external editor");
        println!("\nInformational");
        println!("  \\d                     list tables (simple)");
        println!("  \\d+                    list tables with stats (detailed)");
        println!("  \\d TABLE              describe table (show columns and indexes)");
        println!("  \\di                    list all indexes (simple)");
        println!("  \\di+                   list all indexes with stats (detailed)");
        println!("  \\di [TABLE.]INDEX     describe index");
        println!("  \\dt                    list tables (simple)");
        println!("  \\dt+                   list tables with stats (detailed)");
        println!("  \\dj [TABLE]            list jobs, optionally only those for TABLE");
        println!("  \\dj+ [TABLE]           same as \\dj");
        println!("\nRepeat");
        println!("  \\watch [SEC]           re-run the last query or listing every SEC seconds");
        println!(
            "  \\watch [SEC] STMT      watch STMT (a SQL query or \\d-family listing) instead"
        );
        println!("                         (default 2s; Ctrl-C to stop)");
        println!("\nConnection");
        println!("  \\c [DBNAME]            connect to database");
        println!("\nSQL");
        println!("  SQL queries end with semicolon (;)");
        println!("  Use Ctrl+C to cancel current input");
        println!("  Use Ctrl+D to exit");
        println!("\nOutput");
        println!("  Large results are automatically displayed in pager");
        println!("  Set NO_PAGER=1 to disable pager");
        println!();
    }

    async fn list_tables(&mut self, detailed: bool) -> Result<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("Not connected to LanceDB"))?;

        let tables = Self::table_names(connection).await?;

        if tables.is_empty() {
            println!("Did not find any relations.");
            return Ok(());
        }

        if detailed {
            // For \d+ and \dt+, we need to show detailed table information with stats
            // Fetch table info for each table
            let mut table_infos = Vec::new();
            for table_name in &tables {
                let Some(table) = or_warn(
                    connection.open_table(table_name).execute().await,
                    &format!("opening table '{table_name}'"),
                ) else {
                    table_infos.push(TableInfo {
                        name: table_name.clone(),
                        schema: Arc::new(Schema::empty()),
                        row_count: None,
                        size_bytes: None,
                        num_indices: None,
                        num_fragments: None,
                    });
                    continue;
                };

                let schema = or_warn(
                    table.schema().await,
                    &format!("reading schema for table '{table_name}'"),
                )
                .unwrap_or_else(|| Arc::new(Schema::empty()));
                let stats = or_warn(
                    table.stats().await,
                    &format!("reading stats for table '{table_name}'"),
                );
                let row_count = match stats.as_ref() {
                    Some(s) => Some(s.num_rows as u64),
                    None => or_warn(
                        table.count_rows(None).await,
                        &format!("counting rows for table '{table_name}'"),
                    )
                    .map(|n| n as u64),
                };

                table_infos.push(TableInfo {
                    name: table_name.clone(),
                    schema,
                    row_count,
                    size_bytes: stats.as_ref().map(|s| s.total_bytes as u64),
                    num_indices: stats.as_ref().map(|s| s.num_indices as u64),
                    num_fragments: stats
                        .as_ref()
                        .map(|s| s.fragment_stats.num_fragments as u64),
                });
            }

            // Use the formatter to display the results
            let result = QueryResult::TableListDetailed(table_infos);
            let output = self.formatter.format(&result)?;
            display_with_pager(&output, self.interactive_mode)?;
        } else {
            // Simple list without stats
            let result = QueryResult::TableList(tables);
            let output = self.formatter.format(&result)?;
            display_with_pager(&output, self.interactive_mode)?;
        }
        Ok(())
    }

    async fn describe_table(&mut self, table_name: &str) -> Result<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("Not connected to LanceDB"))?;

        let table = connection
            .open_table(table_name)
            .execute()
            .await
            .map_err(|e| anyhow!("Failed to open table '{}': {}", table_name, e))?;

        let schema = table
            .schema()
            .await
            .map_err(|e| anyhow!("Failed to get schema: {}", e))?;

        let stats = table.stats().await.ok();
        let row_count = if let Some(s) = stats.as_ref() {
            Some(s.num_rows as u64)
        } else {
            table.count_rows(None).await.ok().map(|n| n as u64)
        };

        // Get index information
        let indices = table.list_indices().await.ok();
        let mut index_info = Vec::new();
        if let Some(indices) = indices {
            for index in indices {
                let mut info = IndexInfo {
                    name: index.name.clone(),
                    table_name: table_name.to_string(),
                    index_type: index.index_type.to_string(),
                    columns: index.columns.clone(),
                };

                // Try to get distance type for vector indices
                if let Ok(Some(stats)) = table.index_stats(&index.name).await {
                    if let Some(distance_type) = stats.distance_type {
                        info.index_type =
                            format!("{} distance={}", index.index_type, distance_type);
                    }
                }

                index_info.push(info);
            }
        }

        let table_info = TableInfo {
            name: table_name.to_string(),
            schema,
            row_count,
            size_bytes: stats.as_ref().map(|s| s.total_bytes as u64),
            num_indices: stats.as_ref().map(|s| s.num_indices as u64),
            num_fragments: stats
                .as_ref()
                .map(|s| s.fragment_stats.num_fragments as u64),
        };

        let result = QueryResult::TableInfoWithIndices(table_info, index_info);
        let output = self.formatter.format(&result)?;
        display_with_pager(&output, self.interactive_mode)?;
        Ok(())
    }

    async fn list_all_indexes(&mut self, detailed: bool) -> Result<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("Not connected to LanceDB"))?;

        let tables = Self::table_names(connection).await?;

        // Each opened table is kept alongside its indexes because the `\di+`
        // pass below needs the same handle: reopening per index would issue one
        // request per index where one per table suffices.
        let mut per_table = Vec::new();
        for table_name in tables {
            // A failure here drops the table from the listing entirely, so it
            // has to be reported: an incomplete index list is indistinguishable
            // from a table that genuinely has no indexes.
            let Some(table) = or_warn(
                connection.open_table(&table_name).execute().await,
                &format!("opening table '{table_name}'"),
            ) else {
                continue;
            };
            let Some(indices) = or_warn(
                table.list_indices().await,
                &format!("listing indexes for table '{table_name}'"),
            ) else {
                continue;
            };
            let infos: Vec<IndexInfo> = indices
                .into_iter()
                .map(|index| IndexInfo {
                    name: index.name,
                    table_name: table_name.clone(),
                    index_type: index.index_type.to_string(),
                    columns: index.columns,
                })
                .collect();
            per_table.push((table_name, table, infos));
        }

        if detailed {
            // Fetch stats for each index
            let mut indices_with_stats = Vec::new();
            for (table_name, table, infos) in per_table {
                for index in infos {
                    // `Ok(None)` is an index that legitimately has no stats;
                    // only `Err` is a failed request. Collapsing the two is what
                    // makes a failure read as an index without statistics.
                    let stats = or_warn(
                        table.index_stats(&index.name).await,
                        &format!(
                            "reading stats for index '{}' on table '{}'",
                            index.name, table_name
                        ),
                    )
                    .flatten()
                    .map(|s| IndexStats {
                        num_indexed_rows: s.num_indexed_rows as u64,
                        num_unindexed_rows: s.num_unindexed_rows as u64,
                        distance_type: s.distance_type.map(|dt| dt.to_string()),
                    });
                    indices_with_stats.push((index, stats));
                }
            }
            let result = QueryResult::IndexListDetailed(indices_with_stats);
            let output = self.formatter.format(&result)?;
            display_with_pager(&output, self.interactive_mode)?;
        } else {
            let all_indices: Vec<IndexInfo> = per_table
                .into_iter()
                .flat_map(|(_, _, infos)| infos)
                .collect();
            let result = QueryResult::IndexList(all_indices);
            let output = self.formatter.format(&result)?;
            display_with_pager(&output, self.interactive_mode)?;
        }
        Ok(())
    }

    async fn describe_index(&mut self, index_name: &str, detailed: bool) -> Result<()> {
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow!("Not connected to LanceDB"))?;

        // Support qualified table.index_name syntax
        if let Some((table_name, idx_name)) = index_name.split_once('.') {
            let table = connection
                .open_table(table_name)
                .execute()
                .await
                .map_err(|_| anyhow!("Table \"{}\" not found.", table_name))?;
            let indices = table.list_indices().await?;
            for index in indices {
                if index.name == idx_name {
                    Self::print_index_details(&index, table_name, detailed, &table).await;
                    return Ok(());
                }
            }
            eprintln!(
                "Did not find index \"{}\" on table \"{}\".",
                idx_name, table_name
            );
            return Ok(());
        }

        // Unqualified: search all tables
        let tables = Self::table_names(connection).await?;
        let mut found_count = 0u32;

        for table_name in &tables {
            if let Ok(table) = connection.open_table(table_name).execute().await {
                if let Ok(indices) = table.list_indices().await {
                    for index in &indices {
                        if index.name == index_name {
                            if found_count == 0 {
                                Self::print_index_details(index, table_name, detailed, &table)
                                    .await;
                            }
                            found_count += 1;
                        }
                    }
                }
            }
        }

        if found_count == 0 {
            eprintln!("Did not find any index named \"{}\".", index_name);
        } else if found_count > 1 {
            eprintln!(
                "\nNote: {} indexes named \"{}\" exist across different tables. \
                 Use \\di TABLE.INDEX to specify.",
                found_count, index_name
            );
        }
        Ok(())
    }

    async fn print_index_details(
        index: &lancedb::index::IndexConfig,
        table_name: &str,
        detailed: bool,
        table: &lancedb::Table,
    ) {
        println!("Index \"{}\"", index.name);
        println!("  Table: {}", table_name);
        println!("  Type: {}", index.index_type);
        println!("  Columns: {}", index.columns.join(", "));

        if detailed {
            if let Ok(Some(stats)) = table.index_stats(&index.name).await {
                println!("\nStatistics:");
                println!("  Indexed rows: {}", stats.num_indexed_rows);
                println!("  Unindexed rows: {}", stats.num_unindexed_rows);
                if let Some(dist) = stats.distance_type {
                    println!("  Distance type: {}", dist);
                }
            }
        }
    }
}

// Compatibility functions for tests that use writer
pub async fn run_client_with_writer(
    conn: &ResolvedConnection,
    writer: Option<Box<dyn std::io::Write + Send + 'static>>,
) -> anyhow::Result<()> {
    // If writer is provided, we need different behavior for tests
    // For now, just ignore the writer and use the new implementation
    let _ = writer;

    // Create concrete implementations
    let sql_executor =
        SdkSqlExecutor::new_with_authentication(String::new(), conn.authentication.clone())
            .with_tls(conn.tls.clone());

    let mut client = LanceDBClient::new(
        Some(conn.database.clone()),
        true,
        OutputFormat::Table,
        conn.authentication.clone(),
        conn.profile_name.clone(),
        sql_executor,
    )
    .await?;

    if !conn.sql_server_url.is_empty() {
        client.connect(conn).await?;
    }

    client.run_repl().await
}

pub async fn run_client_command_with_writer(
    conn: &ResolvedConnection,
    command: String,
    format: String,
    writer: Option<Box<dyn std::io::Write + Send + 'static>>,
) -> anyhow::Result<()> {
    // If writer is provided, we need to capture output
    let _ = writer;

    let output_format = format.parse::<OutputFormat>()?;

    // Create concrete implementations
    let sql_executor =
        SdkSqlExecutor::new_with_authentication(String::new(), conn.authentication.clone())
            .with_tls(conn.tls.clone());
    let mut client = LanceDBClient::new(
        Some(conn.database.clone()),
        false,
        output_format,
        conn.authentication.clone(),
        conn.profile_name.clone(),
        sql_executor,
    )
    .await?;

    if !conn.sql_server_url.is_empty() {
        client.connect(conn).await?;
    }

    let trimmed = command.trim();
    if trimmed.starts_with('\\') {
        client.handle_metacommand(trimmed).await?;
    } else {
        client.execute_query(&command).await?;
    }

    Ok(())
}

/// SQL lexical rules for scripts. Comments, including MySQL-style hints, must
/// remain comments when deciding which statements to submit independently.
#[derive(Debug, Default)]
struct ScriptDialect(GenericDialect);

impl Dialect for ScriptDialect {
    fn dialect(&self) -> std::any::TypeId {
        self.0.dialect()
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.0.is_delimited_identifier_start(ch)
    }

    fn supports_nested_comments(&self) -> bool {
        true
    }

    fn supports_multiline_comment_hints(&self) -> bool {
        false
    }

    fn supports_string_escape_constant(&self) -> bool {
        true
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }
}

/// Split only on SQL delimiter tokens, retaining the original spelling of each
/// statement. Tokenize the whole script before executing any of its statements.
fn split_sql_script(script: &str) -> Result<Vec<&str>> {
    let tokens = Tokenizer::new(&ScriptDialect::default(), script)
        .tokenize_with_location()
        .context("Failed to tokenize SQL script")?;
    // Token locations count characters rather than bytes. Walk the source once
    // to translate delimiter locations without changing Unicode or line endings.
    let mut offsets = script
        .char_indices()
        .scan(Location::new(1, 1), |location, (offset, ch)| {
            let start = *location;
            if ch == '\n' {
                location.line += 1;
                location.column = 1;
            } else {
                location.column += 1;
            }
            Some((start, offset))
        });
    let mut commands = Vec::new();
    let mut start = 0;
    let mut has_statement = false;
    for token in tokens {
        match token.token {
            Token::SemiColon => {
                let (_, end) = offsets
                    .find(|(location, _)| *location == token.span.start)
                    .context("SQL delimiter location is outside the script")?;
                if has_statement {
                    commands.push(script[start..end].trim());
                }
                start = end + 1;
                has_statement = false;
            }
            Token::Whitespace(_) => {}
            _ => has_statement = true,
        }
    }
    if has_statement {
        commands.push(script[start..].trim());
    }
    Ok(commands)
}

pub async fn run_client_script_with_writer(
    conn: &ResolvedConnection,
    script: String,
    format: String,
    writer: Option<Box<dyn std::io::Write + Send + 'static>>,
) -> anyhow::Result<()> {
    let _ = writer;
    let commands = split_sql_script(&script)?;

    let output_format = format.parse::<OutputFormat>()?;

    // Create concrete implementations
    let sql_executor =
        SdkSqlExecutor::new_with_authentication(String::new(), conn.authentication.clone())
            .with_tls(conn.tls.clone());
    let mut client = LanceDBClient::new(
        Some(conn.database.clone()),
        false,
        output_format,
        conn.authentication.clone(),
        conn.profile_name.clone(),
        sql_executor,
    )
    .await?;

    if !conn.sql_server_url.is_empty() {
        client.connect(conn).await?;
    }

    for command in commands {
        if command.starts_with('\\') {
            client.handle_metacommand(command).await?;
        } else {
            client.execute_query(command).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        connect_sql_endpoint, copyable_columns, is_show_jobs, prepare_show_jobs_result,
        MAX_DISPLAYED_JOB_ROWS,
    };
    use crate::formatter::{row_count, OutputFormat, QueryResult, ResultFormatter};
    use crate::mocks::test_mocks::MockSqlExecutor;
    use crate::sql_executor::SqlResult;

    #[tokio::test]
    async fn table_names_follows_short_and_empty_pages_and_sorts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new().fallback(
            |axum::extract::Query(query): axum::extract::Query<
                std::collections::HashMap<String, String>,
            >| async move {
                assert_eq!(query.get("limit").map(String::as_str), Some("100"));
                let page = match query.get("page_token").map(String::as_str) {
                    None => serde_json::json!({"tables": ["zebra"], "page_token": "empty"}),
                    Some("empty") => serde_json::json!({"tables": [], "page_token": "last"}),
                    Some("last") => serde_json::json!({"tables": ["apple"], "page_token": ""}),
                    token => panic!("unexpected page token: {token:?}"),
                };
                axum::Json(page)
            },
        );
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let connection = lancedb::connect("db://test")
            .api_key("test")
            .region("self-hosted")
            .host_override(&endpoint)
            .execute()
            .await
            .unwrap();

        let tables = super::LanceDBClient::<super::SdkSqlExecutor>::table_names(&connection).await;
        server.abort();
        assert_eq!(tables.unwrap(), ["apple", "zebra"]);
    }

    #[tokio::test]
    async fn http_connection_failure_preserves_status() {
        for status in [
            axum::http::StatusCode::UNAUTHORIZED,
            axum::http::StatusCode::FORBIDDEN,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let app = axum::Router::new().fallback(move || async move { status });
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let connection = lancedb::connect("db://test")
                .api_key("test")
                .region("self-hosted")
                .host_override(&endpoint)
                .execute()
                .await
                .unwrap();
            let error = connection
                .list_tables(
                    &[],
                    lancedb::listing::ListingOptions::default().page_limit(1),
                )
                .next_page()
                .await
                .unwrap_err();
            server.abort();
            let message = super::connection_failure_message(&endpoint, "http", &error.into());
            assert!(message.contains(status.as_str()), "{message}");
            assert!(message.contains(&format!("{endpoint} (http)")), "{message}");
            assert!(!message.contains("connection refused"), "{message}");
        }
    }

    #[tokio::test]
    async fn sql_connection_failure_preserves_transport_cause() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut executor = super::SdkSqlExecutor::new_with_api_key(String::new(), String::new());
        let (_, error) = connect_sql_endpoint(&mut executor, &endpoint, "test")
            .await
            .unwrap();
        let message = super::connection_failure_message(&endpoint, "sql", &error.unwrap());
        assert!(
            message.to_lowercase().contains("connection refused"),
            "{message}"
        );
        assert!(message.contains(&format!("{endpoint} (sql)")), "{message}");
    }

    /// The scheme a user configures must survive all the way to the transport;
    /// the CLI used to rebuild every address as plaintext http.
    #[tokio::test]
    async fn sql_endpoint_preserves_the_configured_scheme() {
        for (configured, dialed) in [
            ("localhost:10025", "http://localhost:10025"),
            ("localhost", "http://localhost:10025"),
            ("http://sql.example.com", "http://sql.example.com:80"),
            ("https://sql.example.com", "https://sql.example.com:443"),
            (
                "https://sql.example.com:10025",
                "https://sql.example.com:10025",
            ),
            (
                "grpc://sql.example.com:10025",
                "http://sql.example.com:10025",
            ),
            (
                "grpcs://sql.example.com:10025",
                "https://sql.example.com:10025",
            ),
            ("grpcs://[fd00::1]:10025", "https://[fd00::1]:10025"),
        ] {
            let mut executor = MockSqlExecutor::new();
            let (url, err) = connect_sql_endpoint(&mut executor, configured, "analytics")
                .await
                .unwrap();

            assert!(err.is_none());
            assert_eq!(url, dialed, "returned url for {configured}");
            assert_eq!(
                executor.get_connected_urls(),
                vec![dialed.to_string()],
                "url handed to the executor for {configured}"
            );
        }
    }

    #[tokio::test]
    async fn sql_endpoint_rejects_an_unusable_address() {
        let mut executor = MockSqlExecutor::new();
        assert!(
            connect_sql_endpoint(&mut executor, "ftp://sql.example.com", "analytics")
                .await
                .is_err()
        );
        // A rejected address must not reach the transport at all.
        assert!(executor.get_connected_urls().is_empty());
    }

    fn jobs(rows: usize) -> SqlResult {
        let ids: Vec<i32> = (0..rows as i32).collect();
        SqlResult::Query(vec![
            arrow_array::record_batch!(("job_id", Int32, ids)).unwrap()
        ])
    }

    #[test]
    fn interactive_table_output_is_cut_and_annotated() {
        let over = MAX_DISPLAYED_JOB_ROWS + 1;
        let (result, notice) = prepare_show_jobs_result(jobs(over), true);
        assert_eq!(row_count(&result), MAX_DISPLAYED_JOB_ROWS);
        let notice = notice.unwrap();
        assert!(notice.contains(&format!("first {MAX_DISPLAYED_JOB_ROWS} of {over} jobs")));
        assert!(notice.contains("SHOW JOBS FOR <table>"));
        assert!(notice.contains("lancedb sql --command 'SHOW JOBS' --format json"));

        // At or under the cap the listing is whole and says nothing.
        let (result, notice) = prepare_show_jobs_result(jobs(MAX_DISPLAYED_JOB_ROWS), true);
        assert_eq!(row_count(&result), MAX_DISPLAYED_JOB_ROWS);
        assert!(notice.is_none());
    }

    /// A script reading `--format json` needs every row and a payload its
    /// parser accepts, so neither the cut nor the notice may reach it.
    #[test]
    fn programmatic_output_keeps_every_row_and_stays_parsable() {
        let over = MAX_DISPLAYED_JOB_ROWS + 1;
        let (result, notice) = prepare_show_jobs_result(jobs(over), false);
        assert_eq!(row_count(&result), over);
        assert!(notice.is_none());

        for format in [OutputFormat::Json, OutputFormat::Ndjson, OutputFormat::Csv] {
            let output = ResultFormatter::new(format)
                .format(&QueryResult::SqlResult(jobs(over)))
                .unwrap();
            assert!(
                !output.contains("Showing the first"),
                "{format:?} output must carry no prose"
            );
        }
    }

    #[test]
    fn show_jobs_is_recognized_through_leading_trivia() {
        assert!(is_show_jobs("SHOW JOBS"));
        assert!(is_show_jobs("  show   jobs ;"));
        assert!(is_show_jobs("-- list jobs\nSHOW JOBS;"));
        assert!(is_show_jobs("/* list jobs */ SHOW JOBS"));
        assert!(is_show_jobs("SHOW JOBS FOR t"));

        assert!(!is_show_jobs("SHOW JOBSX"));
        assert!(!is_show_jobs("SELECT job_id FROM t"));
        // A query that is only a comment names no statement at all.
        assert!(!is_show_jobs("-- SHOW JOBS"));
    }

    #[test]
    fn copyable_columns_follow_the_statement() {
        assert_eq!(copyable_columns("SELECT 1"), &[] as &[&str]);
        assert_eq!(copyable_columns("cancel job 'x'"), &["job_id"]);
        assert_eq!(copyable_columns("/* run */ Cancel Job 'x';"), &["job_id"]);
        assert_eq!(copyable_columns("SHOW FUNCTIONS"), &["version"]);
        assert_eq!(
            copyable_columns("show errors on t"),
            &["job_id", "function_version"]
        );
        assert_eq!(copyable_columns("REFRESH COLUMN c ON t"), &["job_id"]);
        assert_eq!(copyable_columns("REFRESH MATERIALIZED VIEW v"), &["job_id"]);
        assert!(!is_show_jobs("/* SHOW JOBS"));
    }
}

#[cfg(test)]
mod watch_tests {
    use super::*;

    #[test]
    fn d_family_is_watchable() {
        for cmd in [
            "\\d",
            "\\d+",
            "\\dt",
            "\\dt+",
            "\\di",
            "\\di+",
            "\\dj",
            "\\dj+",
            "\\d mytable",
            "\\dj mytable",
        ] {
            assert!(is_watchable_metacommand(cmd), "{cmd} should be watchable");
        }
    }

    #[test]
    fn dj_maps_to_show_jobs() {
        assert_eq!(show_jobs_statement(None), "SHOW JOBS");
        assert_eq!(show_jobs_statement(Some("t")), r#"SHOW JOBS FOR "t""#);
        // Names that do not tokenize as a bare identifier; the parser-side
        // round trip is pinned by `parses_show_jobs_for_quoted_table`.
        assert_eq!(
            show_jobs_statement(Some("my-table")),
            r#"SHOW JOBS FOR "my-table""#
        );
        assert_eq!(show_jobs_statement(Some("a.b")), r#"SHOW JOBS FOR "a.b""#);
        assert_eq!(
            show_jobs_statement(Some(r#"a"b"#)),
            r#"SHOW JOBS FOR "a""b""#
        );
    }

    #[test]
    fn one_shot_and_watch_are_not_watchable() {
        for cmd in [
            "\\watch",
            "\\watch 5",
            "\\c",
            "\\e",
            "\\q",
            "\\?",
            "\\h",
            "",
        ] {
            assert!(
                !is_watchable_metacommand(cmd),
                "{cmd} should not be watchable"
            );
        }
    }

    #[test]
    fn unrecognized_d_prefixed_commands_are_not_watchable() {
        // A `\d`-prefixed typo/unsupported verb is rejected by
        // `handle_metacommand`; it must not be treated as watchable, else it
        // would clobber `last_command` and make `\watch` loop an error.
        for cmd in ["\\dfoo", "\\ds", "\\dn", "\\dd", "\\d++"] {
            assert!(
                !is_watchable_metacommand(cmd),
                "{cmd} should not be watchable"
            );
        }
    }

    #[test]
    fn watch_interval_accepts_only_sane_positive_seconds() {
        // Accept normal intervals, including exactly the floor.
        assert!(watch_interval(2.0).is_some());
        assert!(watch_interval(0.5).is_some());
        assert!(watch_interval(MIN_WATCH_INTERVAL_SECS).is_some());
        // Reject non-positive, non-finite, and overflowing values (which would
        // panic the sleep), plus positive-but-tiny values below the floor that
        // would spin the loop with no real delay (e.g. 1e-20 rounds to a
        // zero-length sleep; 1e-6 to a nonzero-but-negligible 1us).
        for bad in [f64::INFINITY, 1e300, 0.0, -1.0, f64::NAN, 1e-20, 1e-6, 0.05] {
            assert!(watch_interval(bad).is_none(), "{bad} should be rejected");
        }
    }

    #[test]
    fn parse_watch_args_reads_interval_and_optional_statement() {
        // (input after `\watch`, expected secs, expected target)
        let cases: &[(&str, f64, Option<&str>)] = &[
            ("", 2.0, None),
            ("  ", 2.0, None),
            ("5", 5.0, None),
            ("0.5", 0.5, None),
            ("SELECT 1", 2.0, Some("SELECT 1")),
            ("5 SELECT * FROM t", 5.0, Some("SELECT * FROM t")),
            ("2.5   SELECT   1", 2.5, Some("SELECT   1")),
            ("\\di+", 2.0, Some("\\di+")),
            ("3 \\dt+", 3.0, Some("\\dt+")),
        ];
        for (input, secs, target) in cases {
            let got = parse_watch_args(input).unwrap_or_else(|e| panic!("{input:?}: {e}"));
            assert_eq!(
                got.interval,
                Duration::from_secs_f64(*secs),
                "interval for {input:?}"
            );
            assert_eq!(got.target.as_deref(), *target, "target for {input:?}");
        }
    }

    #[test]
    fn parse_watch_args_rejects_numeric_but_invalid_intervals() {
        // A numeric first token is always an interval attempt (never SQL), so
        // an unusable one is an error rather than being treated as a statement.
        for bad in [
            "0",
            "-1",
            "inf",
            "1e300",
            "1e-20",
            "0 SELECT 1",
            "-2 SELECT 1",
            "1e-20 SELECT 1",
        ] {
            assert!(parse_watch_args(bad).is_err(), "{bad:?} should be rejected");
        }
    }
}
