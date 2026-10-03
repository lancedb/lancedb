// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Differential refresh testing.
//!
//! The refresh contract is a property: after any sequence of source
//! mutations, a view maintained by default (incremental-where-possible)
//! refreshes equals the definition evaluated against the source directly,
//! and so does a forced rebuild. The oracle is an independent read of the
//! source -- plain column scan, filter applied in Rust -- so it shares
//! nothing with the refresh path it checks.
//!
//! The oracle runs after every step, not just at the end: a later mutation
//! that forces a rebuild would silently heal an incremental error, and those
//! transient errors are exactly the bugs this exists to catch.

use arrow_array::{Float32Array, Int32Array, RecordBatch};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use futures::{StreamExt, TryStreamExt};
use lance::dataset::NewColumnTransform;
use std::sync::Arc;

use super::MaterializedView;
use super::refresh::RefreshMode;
use crate::connect;
use crate::connection::Connection;
use crate::query::{ExecutableQuery, QueryBase, Select};
use crate::table::{CompactionOptions, OptimizeAction, Table};

/// One source mutation, one per correctness-relevant class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SrcOp {
    /// Fresh non-colliding ids of both parities, so every other op has
    /// view-resident rows to act on: the only op that should refresh
    /// incrementally.
    AppendNew,
    /// Deletion in surviving fragments must break the pure-append check.
    DeleteEven,
    /// An in-place update; on the filtered shape it crosses the predicate,
    /// so rows must leave the view.
    UpdateOddScore,
    /// Fragment rewrite/renumber must break the pure-append check.
    Compact,
    /// A column the view does not read must NOT force a rebuild.
    AddColumn,
    /// merge_insert commits an Update whose by-source arm deletes rows, so a
    /// classifier that reads Update as "changed only" loses those deletions.
    MergeDropLargest,
    /// merge_insert that both changes existing rows and inserts new ones in
    /// one transaction.
    MergeUpsert,
}

const ALL_OPS: [SrcOp; 7] = [
    SrcOp::AppendNew,
    SrcOp::DeleteEven,
    SrcOp::UpdateOddScore,
    SrcOp::Compact,
    SrcOp::AddColumn,
    SrcOp::MergeDropLargest,
    SrcOp::MergeUpsert,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// SELECT id, score.
    Identity,
    /// SELECT id, score WHERE score > 50: additionally sensitive to rows
    /// crossing the predicate.
    Filtered,
    /// SELECT id, score LIMIT 4. Which rows are held depends on the order
    /// they were first materialized, so the oracle checks containment and
    /// the cap rather than equality.
    Limited,
}

impl Shape {
    fn filter(&self) -> Option<&'static str> {
        match self {
            Self::Identity | Self::Limited => None,
            Self::Filtered => Some("score > 50"),
        }
    }

    fn matches(&self, score: f32) -> bool {
        match self {
            Self::Identity | Self::Limited => true,
            Self::Filtered => score > 50.0,
        }
    }

    fn limit(&self) -> Option<usize> {
        match self {
            Self::Limited => Some(4),
            _ => None,
        }
    }
}

struct Case {
    conn: Connection,
    source: Table,
    view: MaterializedView,
    shape: Shape,
    next_id: i32,
    added_columns: u32,
}

fn rows_batch(ids: &[i32]) -> RecordBatch {
    let scores: Vec<f32> = ids.iter().map(|id| (*id * 10) as f32).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, true),
            ArrowField::new("score", DataType::Float32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Float32Array::from(scores)),
        ],
    )
    .unwrap()
}

fn merge_batch(ids: &[i32]) -> RecordBatch {
    let scores: Vec<f32> = ids.iter().map(|id| (*id * 10 + 5) as f32).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, true),
            ArrowField::new("score", DataType::Float32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Float32Array::from(scores)),
        ],
    )
    .unwrap()
}

impl Case {
    async fn new(shape: Shape) -> Self {
        let conn = connect("memory://").execute().await.unwrap();
        let source = conn
            .create_table("src", rows_batch(&[1, 2, 3, 4]))
            .write_options(crate::materialized_view::tests::stable_row_ids())
            .execute()
            .await
            .unwrap();
        let mut builder = conn
            .create_materialized_view("view", "src")
            .select([("id", "id"), ("score", "score")]);
        if let Some(filter) = shape.filter() {
            builder = builder.only_if(filter);
        }
        if let Some(limit) = shape.limit() {
            builder = builder.limit(limit as u64);
        }
        let view = builder.execute().await.unwrap();
        Self {
            conn,
            source,
            view,
            shape,
            next_id: 100,
            added_columns: 0,
        }
    }

    async fn apply(&mut self, op: SrcOp) {
        match op {
            SrcOp::AppendNew => {
                // Mixed parity: the middle id is odd, so UpdateOddScore always
                // has a filter-matching appended row to evict.
                let ids = vec![self.next_id, self.next_id + 101, self.next_id + 202];
                self.next_id += 303;
                self.source.add(rows_batch(&ids)).execute().await.unwrap();
            }
            SrcOp::DeleteEven => {
                self.source.delete("id % 2 = 0").await.unwrap();
            }
            SrcOp::UpdateOddScore => {
                self.source
                    .update()
                    .column("score", "-1.0")
                    .only_if("id % 2 = 1")
                    .execute()
                    .await
                    .unwrap();
            }
            SrcOp::Compact => {
                self.source
                    .optimize(OptimizeAction::Compact {
                        options: CompactionOptions::default(),
                        remap_options: None,
                    })
                    .await
                    .unwrap();
            }
            SrcOp::MergeDropLargest => {
                let mut ids = self.source_ids().await;
                ids.sort_unstable();
                ids.pop();
                if ids.is_empty() {
                    return;
                }
                let batch = rows_batch(&ids);
                let reader =
                    arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
                let mut merge = self.source.merge_insert(&["id"]);
                merge.when_not_matched_by_source_delete(None);
                merge.execute(Box::new(reader)).await.unwrap();
            }
            SrcOp::MergeUpsert => {
                let mut ids = self.source_ids().await;
                ids.sort_unstable();
                // One row that exists (updated in place) and one that does not.
                let existing = ids.first().copied().unwrap_or(self.next_id);
                let fresh = self.next_id;
                self.next_id += 1;
                let batch = merge_batch(&[existing, fresh]);
                let reader =
                    arrow_array::RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
                let mut merge = self.source.merge_insert(&["id"]);
                merge
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                merge.execute(Box::new(reader)).await.unwrap();
            }
            SrcOp::AddColumn => {
                self.added_columns += 1;
                let field = ArrowField::new(
                    format!("extra_{}", self.added_columns),
                    DataType::Int32,
                    true,
                );
                self.source
                    .add_columns()
                    .transform(NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(
                        vec![field],
                    ))))
                    .execute()
                    .await
                    .unwrap();
            }
        }
    }

    async fn source_ids(&self) -> Vec<i32> {
        read_rows(
            self.source
                .query()
                .select(Select::columns(&["id", "score"])),
        )
        .await
        .into_iter()
        .map(|(id, _)| id)
        .collect()
    }

    /// The definition's result, read independently of the refresh path:
    /// plain column scan, filter applied here, sorted.
    async fn oracle(&self) -> Vec<(i32, i32)> {
        let mut rows = read_rows(
            self.source
                .query()
                .select(Select::columns(&["id", "score"])),
        )
        .await
        .into_iter()
        .filter(|(_, score)| self.shape.matches(*score as f32))
        .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    async fn view_rows(&self) -> Vec<(i32, i32)> {
        let mut rows = read_rows(
            self.view
                .table()
                .query()
                .select(Select::columns(&["id", "score"])),
        )
        .await;
        rows.sort_unstable();
        rows
    }

    async fn check(&self, label: &str) -> Result<(), String> {
        let expected = self.oracle().await;
        let actual = self.view_rows().await;
        let Some(cap) = self.shape.limit() else {
            if expected != actual {
                return Err(format!(
                    "{label}: view diverged from oracle\n  expected: {expected:?}\n  actual:   {actual:?}"
                ));
            }
            return Ok(());
        };
        // A capped view holds some subset of the definition's result, never
        // more than the cap, and never the same row twice.
        if actual.len() > cap {
            return Err(format!(
                "{label}: view holds {} rows, over its cap of {cap}: {actual:?}",
                actual.len()
            ));
        }
        let mut unique = actual.clone();
        unique.dedup();
        if unique.len() != actual.len() {
            return Err(format!("{label}: view holds a row twice: {actual:?}"));
        }
        if let Some(stray) = actual.iter().find(|row| !expected.contains(row)) {
            return Err(format!(
                "{label}: view holds {stray:?}, which the definition does not select: {expected:?}"
            ));
        }
        // Below the cap the view must be complete, or a row was lost.
        if actual.len() < cap.min(expected.len()) {
            return Err(format!(
                "{label}: view holds {} of {} selectable rows under a cap of {cap}: {actual:?}",
                actual.len(),
                expected.len()
            ));
        }
        Ok(())
    }
}

async fn read_rows(query: impl ExecutableQuery) -> Vec<(i32, i32)> {
    let batches = query
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            let scores = batch["score"]
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            // Scores are integer-valued by construction; compare exactly.
            (0..batch.num_rows())
                .map(|i| (ids.value(i), scores.value(i) as i32))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Drive one mutation sequence: refresh + oracle-check after every step,
/// then a forced rebuild checked against the same oracle.
async fn run_sequence(ops: &[SrcOp], shape: Shape) -> Result<(), String> {
    let label = format!("{shape:?} {ops:?}");
    let mut case = Case::new(shape).await;
    case.view
        .refresh()
        .execute()
        .await
        .map_err(|e| format!("{label}: initial refresh failed: {e}"))?;
    case.check(&format!("{label} (initial)")).await?;

    for (step, op) in ops.iter().enumerate() {
        case.apply(*op).await;
        case.view
            .refresh()
            .execute()
            .await
            .map_err(|e| format!("{label}: refresh at step {step} failed: {e}"))?;
        case.check(&format!("{label} (step {step}, {op:?})"))
            .await?;
    }

    case.view
        .refresh()
        .full(true)
        .execute()
        .await
        .map_err(|e| format!("{label}: final full refresh failed: {e}"))?;
    case.check(&format!("{label} (final rebuild)")).await?;
    // Silence the unused-connection lint without dropping it mid-case.
    let _ = &case.conn;
    Ok(())
}

/// Every op sequence up to `max_len`.
fn all_sequences(max_len: u32) -> Vec<Vec<SrcOp>> {
    let mut sequences = Vec::new();
    for len in 1..=max_len {
        for mut index in 0..ALL_OPS.len().pow(len) {
            let mut ops = Vec::with_capacity(len as usize);
            for _ in 0..len {
                ops.push(ALL_OPS[index % ALL_OPS.len()]);
                index /= ALL_OPS.len();
            }
            sequences.push(ops);
        }
    }
    sequences
}

async fn run_exhaustive(max_len: u32) {
    let mut cases = Vec::new();
    for shape in [Shape::Identity, Shape::Filtered, Shape::Limited] {
        for ops in all_sequences(max_len) {
            cases.push((ops, shape));
        }
    }
    let failures: Vec<String> = futures::stream::iter(cases)
        .map(|(ops, shape)| async move { run_sequence(&ops, shape).await.err() })
        .buffer_unordered(8)
        .filter_map(|failure| async move { failure })
        .collect()
        .await;
    assert!(
        failures.is_empty(),
        "{} sequences diverged; first: {}",
        failures.len(),
        failures[0]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_exhaustive() {
    run_exhaustive(3).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "longer sweep; run manually"]
async fn differential_exhaustive_deep() {
    run_exhaustive(4).await;
}

/// Named interleavings that double as repro handles. The mode assertions pin
/// the classifier, which value comparison alone cannot: a wrongly rebuilt
/// view still matches the oracle.
#[tokio::test(flavor = "multi_thread")]
async fn differential_named_regressions() {
    // An append is the one op that must stay incremental.
    let mut case = Case::new(Shape::Identity).await;
    case.view.refresh().execute().await.unwrap();
    case.apply(SrcOp::AppendNew).await;
    let result = case.view.refresh().execute().await.unwrap();
    assert_eq!(result.mode, RefreshMode::Incremental);
    case.check("append stays incremental").await.unwrap();

    // A column the view does not read must not force a rebuild.
    let mut case = Case::new(Shape::Identity).await;
    case.view.refresh().execute().await.unwrap();
    case.apply(SrcOp::AddColumn).await;
    let result = case.view.refresh().execute().await.unwrap();
    assert_eq!(result.mode, RefreshMode::Incremental);
    assert_eq!(result.rows_written, 0);

    // Compaction rearranges rows without changing them: the watermark
    // advances and nothing rebuilds.
    let mut case = Case::new(Shape::Identity).await;
    case.view.refresh().execute().await.unwrap();
    case.apply(SrcOp::AppendNew).await;
    case.view.refresh().execute().await.unwrap();
    case.apply(SrcOp::Compact).await;
    let result = case.view.refresh().execute().await.unwrap();
    assert_eq!(result.mode, RefreshMode::Incremental);
    assert_eq!(result.rows_written, 0);
    case.check("compaction alone").await.unwrap();

    // Fragment bookkeeping stays coherent across the compaction: the next
    // append is separable and computed alone.
    case.apply(SrcOp::AppendNew).await;
    let result = case.view.refresh().execute().await.unwrap();
    assert_eq!(result.mode, RefreshMode::Incremental);
    assert_eq!(result.rows_written, 3);
    case.check("compact then append").await.unwrap();

    // A row updated to no longer match the filter must leave the view --
    // and the fixture must prove the eviction happened, not merely that the
    // end state matches: an update that never touched a view-resident row
    // would also "match".
    let mut case = Case::new(Shape::Filtered).await;
    case.apply(SrcOp::AppendNew).await;
    case.view.refresh().execute().await.unwrap();
    let before = case.view_rows().await.len();
    case.apply(SrcOp::UpdateOddScore).await;
    case.view.refresh().execute().await.unwrap();
    let after = case.view_rows().await.len();
    assert!(
        after < before,
        "no view-resident row was evicted ({before} -> {after}); the fixture \
         no longer exercises the filtered-update transition"
    );
    case.check("update crosses the filter").await.unwrap();
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------
//
// The sequential cases above cannot observe a cross-process race: the
// per-view refresh lock is process-local, so a second refresh in this
// process queues behind the first. What is missing is not more op
// sequences but a second process. These cases add one, and assert the same
// property the harness always asserts -- the view holds each row once.

/// Rows the definition selects from the source: every id but the first,
/// read straight from the source, sharing nothing with the refresh path.
async fn concurrency_oracle(conn: &Connection) -> Vec<i32> {
    let batches: Vec<RecordBatch> = conn
        .open_table("src")
        .execute()
        .await
        .unwrap()
        .query()
        .select(Select::columns(&["id"]))
        .execute()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if column.value(i) > 1 {
                ids.push(column.value(i));
            }
        }
    }
    ids.sort_unstable();
    ids
}

/// The view's ids, sorted.
async fn concurrency_view_ids(conn: &Connection) -> Vec<i32> {
    let batches: Vec<RecordBatch> = conn
        .open_table("mv")
        .execute()
        .await
        .unwrap()
        .query()
        .select(Select::columns(&["id"]))
        .execute()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            ids.push(column.value(i));
        }
    }
    ids.sort_unstable();
    ids
}

/// One refresh of the view at `MV_RACE_DIR`, in its own process.
///
/// Setup happens before the start barrier so warm-up does not stagger the
/// two processes. What makes the race certain rather than likely is the
/// second barrier inside `refresh()` itself, which holds every participant
/// between staging and commit.
#[tokio::test]
#[ignore = "spawned as a child process by the concurrency cases"]
async fn cross_process_refresh_child() {
    let Ok(dir) = std::env::var("MV_RACE_DIR") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    let tag = std::env::var("MV_RACE_TAG").unwrap();

    let conn = connect(dir.to_str().unwrap()).execute().await.unwrap();
    let table = conn.open_table("mv").execute().await.unwrap();
    let _ = table.schema().await.unwrap();
    let _ = table.count_rows(None).await.unwrap();
    let source = conn.open_table("src").execute().await.unwrap();
    let _ = source.count_rows(None).await.unwrap();
    let view = MaterializedView::from_table(table).await.unwrap();

    std::fs::write(dir.join(format!("ready-{tag}")), b"1").unwrap();
    while !dir.join("START").exists() {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }

    let outcome = match view.refresh().execute().await {
        Ok(result) => format!("committed rows={}", result.rows_written),
        Err(err) if is_commit_conflict(&err) => "conflicted".to_string(),
        Err(err) => format!("failed {err}"),
    };
    std::fs::write(dir.join(format!("outcome-{tag}")), outcome).unwrap();
}

/// Whether a refresh lost its commit to a concurrent one, as opposed to
/// failing for any other reason.
fn is_commit_conflict(err: &crate::Error) -> bool {
    let text = err.to_string();
    text.contains("Retryable commit conflict") || text.contains("preempted by concurrent")
}

/// Two processes refreshing one view concurrently must leave the view
/// equal to the oracle: each selected row present exactly once.
///
/// Both plan the same incremental delta from one watermark. A refresh is
/// meant to land on the generation it planned or leave nothing behind, so
/// at most one of them may write.
#[tokio::test]
async fn concurrent_refreshes_hold_each_row_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let conn = connect(&path).execute().await.unwrap();
    conn.create_table("src", rows_batch(&[1, 2, 3, 4]))
        .write_options(crate::materialized_view::tests::stable_row_ids())
        .execute()
        .await
        .unwrap();
    let view = conn
        .create_materialized_view("mv", "src")
        .select([("id", "id"), ("score", "score")])
        .only_if("id > 1")
        .execute()
        .await
        .unwrap();
    // Seed the watermark so the racing refreshes are both incremental.
    view.refresh().execute().await.unwrap();

    // Large enough that a refresh is real work rather than a formality.
    let ids: Vec<i32> = (100..200_100).collect();
    conn.open_table("src")
        .execute()
        .await
        .unwrap()
        .add(rows_batch(&ids))
        .execute()
        .await
        .unwrap();

    let tags = ["a", "b"];
    let exe = std::env::current_exe().unwrap();
    let children: Vec<std::process::Child> = tags
        .iter()
        .map(|tag| {
            std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "materialized_view::differential::cross_process_refresh_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("MV_RACE_DIR", dir.path())
                .env("MV_RACE_SYNC", dir.path())
                .env("MV_RACE_PEERS", "2")
                .env("MV_RACE_TAG", tag)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap()
        })
        .collect();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    while tags
        .iter()
        .any(|tag| !dir.path().join(format!("ready-{tag}")).exists())
    {
        assert!(
            std::time::Instant::now() < deadline,
            "children never became ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    std::fs::write(dir.path().join("START"), b"1").unwrap();
    for (tag, mut child) in tags.iter().zip(children) {
        let status = loop {
            match child.try_wait().unwrap() {
                Some(status) => break status,
                None if std::time::Instant::now() >= deadline => {
                    child.kill().unwrap();
                    panic!("child {tag} never finished");
                }
                None => std::thread::sleep(std::time::Duration::from_millis(10)),
            }
        };
        assert!(status.success(), "child {tag} exited {status}");
    }

    // Both refreshes reached the commit boundary before either committed --
    // the in-refresh barrier guarantees it -- so exactly one may win.
    let outcomes: Vec<String> = tags
        .iter()
        .map(|tag| {
            std::fs::read_to_string(dir.path().join(format!("outcome-{tag}")))
                .unwrap_or_else(|_| panic!("child {tag} recorded no outcome"))
        })
        .collect();
    for tag in tags {
        assert!(
            dir.path().join(format!("planned-{tag}")).exists(),
            "child {tag} never reached the commit boundary, so nothing was synchronized"
        );
    }
    let committed = outcomes.iter().filter(|o| o.contains("committed")).count();
    let conflicted = outcomes.iter().filter(|o| o.contains("conflicted")).count();
    assert_eq!(
        (committed, conflicted),
        (1, 1),
        "exactly one refresh may win the generation both planned: {outcomes:?}"
    );

    let expected = concurrency_oracle(&conn).await;
    let actual = concurrency_view_ids(&conn).await;
    assert_eq!(
        actual.len(),
        expected.len(),
        "the view holds {} rows, the oracle {}: a losing refresh left rows behind",
        actual.len(),
        expected.len()
    );
    assert_eq!(actual, expected, "the view does not match the oracle");
}
