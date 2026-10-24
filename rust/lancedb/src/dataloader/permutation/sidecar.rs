// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Private disk-backed logical occurrence correlation. No sidecar columns enter
//! the original permutation sorter. Correlation sorts run sequentially afterward.
use std::{
    io::{BufReader, BufWriter},
    sync::{Arc, Mutex},
};

use arrow_array::{Array, RecordBatch, UInt64Array};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::{
    disk_manager::{DiskManager, DiskManagerBuilder, RefCountedTempFile},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_expr::col;
use datafusion_physical_plan::{displayable, execute_stream};
use futures::TryStreamExt;
use lance_core::ROW_ID;
use lance_datafusion::exec::SessionContextExt;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::{builder::SRC_ROW_ID_COL, split::SPLIT_ID_COLUMN, util::TemporaryDirectory};
use crate::{
    Error, Result, Table,
    arrow::{SendableRecordBatchStream, SendableRecordBatchStreamExt, SimpleRecordBatchStream},
    query::{ExecutableQuery, QueryBase, Select},
};

pub const IDENTITY_KEY: &str = "checkpoint_plan_identity";
// Private scratch policy, matching DataFusion's default disk limit. Shared by
// source/plan spools and sequential correlation sorts, excluding the unchanged
// original permutation sorter/shuffler. A limit error aborts without an identity.
pub(super) const DISK_LIMIT_BYTES: u64 = 100 * 1024 * 1024 * 1024;
const BATCH_ROWS: usize = 1024;
const IO_BYTES: usize = 64 * 1024;
const DOMAIN: &[u8] = b"lancedb-logical-occurrences-v1\0";
type Completion = Arc<Mutex<Option<Spool>>>;

fn fail(message: impl Into<String>) -> Error {
    Error::Other {
        message: message.into(),
        source: None,
    }
}
fn io(error: std::io::Error) -> Error {
    fail(format!("checkpoint sidecar IO: {error}"))
}
fn schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::UInt64, false))
            .collect::<Vec<_>>(),
    ))
}
fn validate_batch(batch: &RecordBatch, expected: &SchemaRef) -> Result<()> {
    if batch.schema() != *expected
        || batch.num_rows() > BATCH_ROWS
        || batch.columns().iter().any(|c| c.null_count() != 0)
    {
        return Err(fail("invalid checkpoint sidecar batch"));
    }
    Ok(())
}

/// Receipt exists only after explicit Finish, flushed IPC EOS and disk accounting.
pub(super) struct Spool {
    file: RefCountedTempFile,
    schema: SchemaRef,
    rows: u64,
    bytes: u64,
}
struct Writer {
    sender: mpsc::Sender<Option<RecordBatch>>,
    task: tokio::task::JoinHandle<Result<Spool>>,
}
impl Writer {
    fn new(disk: &Arc<DiskManager>, schema: SchemaRef) -> Result<Self> {
        let mut file = disk.create_tmp_file("checkpoint sidecar")?;
        let (sender, mut receiver) = mpsc::channel::<Option<RecordBatch>>(1);
        // The actor owns the file and its parent-directory lifetime even if the
        // async caller drops. In-flight OS IO is not synchronously interruptible.
        let task = tokio::task::spawn_blocking(move || {
            let output = BufWriter::with_capacity(IO_BYTES, file.inner().reopen().map_err(io)?);
            let mut writer = StreamWriter::try_new(output, &schema)?;
            writer.flush()?;
            file.update_disk_usage()?;
            let mut rows = 0u64;
            while let Some(message) = receiver.blocking_recv() {
                match message {
                    Some(batch) => {
                        validate_batch(&batch, &schema)?;
                        rows = rows
                            .checked_add(batch.num_rows() as u64)
                            .ok_or_else(|| fail("sidecar row count overflow"))?;
                        writer.write(&batch)?;
                        writer.flush()?;
                        // Post-write cap: overshoot is at most one fixed batch.
                        // Any failure aborts the entire private runtime; never retry
                        // accounting after its failed cap check.
                        file.update_disk_usage()?;
                    }
                    None => {
                        writer.finish()?;
                        file.update_disk_usage()?;
                        let bytes = file.inner().as_file().metadata().map_err(io)?.len();
                        drop(writer);
                        return Ok(Spool {
                            file,
                            schema,
                            rows,
                            bytes,
                        });
                    }
                }
            }
            Err(fail("sidecar writer cancelled before Finish"))
        });
        Ok(Self { sender, task })
    }
    async fn write(&self, batch: RecordBatch) -> Result<()> {
        self.sender
            .send(Some(batch))
            .await
            .map_err(|_| fail("sidecar writer stopped"))
    }
    async fn finish(self) -> Result<Spool> {
        // Preserve the actor's IO/accounting error even when the send fails.
        let _ = self.sender.send(None).await;
        drop(self.sender);
        self.task
            .await
            .map_err(|e| fail(format!("sidecar writer task: {e}")))?
    }
}

enum ReadEvent {
    Batch(RecordBatch),
    Done,
}
impl Spool {
    fn read(self) -> SendableRecordBatchStream {
        let output_schema = self.schema.clone();
        let (sender, receiver) = mpsc::channel::<Result<ReadEvent>>(1);
        tokio::task::spawn_blocking(move || {
            let result = (|| -> Result<()> {
                if self.file.inner().as_file().metadata().map_err(io)?.len() != self.bytes {
                    return Err(fail("sidecar receipt byte length mismatch"));
                }
                let input =
                    BufReader::with_capacity(IO_BYTES, self.file.inner().reopen().map_err(io)?);
                let mut reader = StreamReader::try_new(input, None)?;
                if reader.schema() != self.schema {
                    return Err(fail("sidecar receipt schema mismatch"));
                }
                let mut rows = 0u64;
                for batch in &mut reader {
                    let batch = batch?;
                    validate_batch(&batch, &self.schema)?;
                    rows = rows
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| fail("sidecar read count overflow"))?;
                    if rows > self.rows {
                        return Err(fail("sidecar receipt row count exceeded"));
                    }
                    if sender.blocking_send(Ok(ReadEvent::Batch(batch))).is_err() {
                        return Ok(());
                    }
                }
                if rows != self.rows {
                    return Err(fail("sidecar receipt row count mismatch"));
                }
                Ok(())
            })();
            // Release the file before publishing the checked terminal receipt.
            drop(self);
            match result {
                Ok(()) => {
                    let _ = sender.blocking_send(Ok(ReadEvent::Done));
                }
                Err(error) => {
                    let _ = sender.blocking_send(Err(error));
                }
            }
        });
        let stream = futures::stream::try_unfold(receiver, |mut receiver| async move {
            match receiver.recv().await {
                Some(Ok(ReadEvent::Batch(batch))) => Ok(Some((batch, receiver))),
                Some(Ok(ReadEvent::Done)) => Ok(None),
                Some(Err(error)) => Err(error),
                None => Err(fail("sidecar reader stopped before checked EOF")),
            }
        });
        Box::pin(SimpleRecordBatchStream {
            schema: output_schema,
            stream,
        })
    }
}

struct Cursor<const N: usize> {
    stream: SendableRecordBatchStream,
    batch: Option<RecordBatch>,
    index: usize,
}
impl<const N: usize> Cursor<N> {
    fn new(spool: Spool) -> Self {
        Self {
            stream: spool.read(),
            batch: None,
            index: 0,
        }
    }
    async fn next(&mut self) -> Result<Option<[u64; N]>> {
        loop {
            if let Some(batch) = &self.batch
                && self.index < batch.num_rows()
            {
                let row = std::array::from_fn(|i| {
                    batch
                        .column(i)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(self.index)
                });
                self.index += 1;
                return Ok(Some(row));
            }
            self.batch = self.stream.try_next().await?;
            self.index = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
    }
}

pub(super) struct Correlation {
    ctx: SessionContext,
}
impl Correlation {
    pub fn new(temp: &TemporaryDirectory, memory_limit: usize, disk_limit: u64) -> Result<Self> {
        if matches!(temp, TemporaryDirectory::None) {
            return Err(fail("checkpoint sidecar requires temporary disk storage"));
        }
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(temp.to_disk_manager_mode())
                    .with_max_temp_directory_size(disk_limit),
            )
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::default()
                .with_target_partitions(1)
                .with_batch_size(BATCH_ROWS),
            runtime,
        );
        Ok(Self { ctx })
    }
    fn disk(&self) -> Arc<DiskManager> {
        self.ctx.runtime_env().disk_manager.clone()
    }

    pub fn tap(
        &self,
        input: SendableRecordBatchStream,
    ) -> Result<(SendableRecordBatchStream, Completion)> {
        let original_schema = input.schema();
        let row_id = original_schema.index_of(ROW_ID)?;
        let sidecar_schema = schema(&["row_id", "ordinal"]);
        let writer = Writer::new(&self.disk(), sidecar_schema.clone())?;
        let completion = Arc::new(Mutex::new(None));
        let state = (input, writer, 0u64, completion.clone());
        let stream = futures::stream::try_unfold(
            state,
            move |(mut input, writer, mut ordinal, completion)| {
                let sidecar_schema = sidecar_schema.clone();
                async move {
                    match input.try_next().await? {
                        Some(batch) => {
                            for offset in (0..batch.num_rows()).step_by(BATCH_ROWS) {
                                let length = BATCH_ROWS.min(batch.num_rows() - offset);
                                let end = ordinal
                                    .checked_add(length as u64)
                                    .ok_or_else(|| fail("eligible ordinal overflow"))?;
                                let ids = batch.column(row_id).slice(offset, length);
                                let ordinals =
                                    Arc::new(UInt64Array::from_iter_values(ordinal..end));
                                writer
                                    .write(RecordBatch::try_new(
                                        sidecar_schema.clone(),
                                        vec![ids, ordinals],
                                    )?)
                                    .await?;
                                ordinal = end;
                            }
                            // Do not rechunk, project or change this original batch.
                            Ok(Some((batch, (input, writer, ordinal, completion))))
                        }
                        None => {
                            let spool = writer.finish().await?;
                            *completion.lock().unwrap() = Some(spool);
                            Ok(None)
                        }
                    }
                }
            },
        );
        Ok((
            Box::pin(SimpleRecordBatchStream {
                schema: original_schema,
                stream,
            }),
            completion,
        ))
    }

    async fn spool(&self, mut stream: SendableRecordBatchStream) -> Result<Spool> {
        let writer = Writer::new(&self.disk(), stream.schema())?;
        while let Some(batch) = stream.try_next().await? {
            for offset in (0..batch.num_rows()).step_by(BATCH_ROWS) {
                writer
                    .write(batch.slice(offset, BATCH_ROWS.min(batch.num_rows() - offset)))
                    .await?;
            }
        }
        writer.finish().await
    }
    async fn sort(&self, input: Spool, keys: &[&str]) -> Result<Spool> {
        let rows = input.rows;
        let frame = self
            .ctx
            .read_one_shot(input.read().into_df_stream())?
            .sort_by(keys.iter().map(|key| col(*key)).collect())?;
        let plan = frame.create_physical_plan().await?;
        log::debug!(
            "checkpoint sidecar physical plan:\n{}",
            displayable(plan.as_ref()).indent(true)
        );
        let stream = execute_stream(plan.clone(), self.ctx.task_ctx())?.map_err(Error::from);
        let output = self
            .spool(Box::pin(SimpleRecordBatchStream {
                schema: plan.schema(),
                stream,
            }))
            .await?;
        if output.rows != rows {
            return Err(fail("sidecar sort changed cardinality"));
        }
        fn report(plan: &dyn datafusion_physical_plan::ExecutionPlan) {
            log::debug!(
                "checkpoint sidecar completed {} metrics: {:?}",
                plan.name(),
                plan.metrics()
            );
            for child in plan.children() {
                report(child.as_ref());
            }
        }
        report(plan.as_ref());
        // No stage starts until the preceding stream has fully drained and drops.
        drop(plan);
        drop(frame);
        Ok(output)
    }

    async fn final_positions(&self, table: &Table) -> Result<Spool> {
        let mut stream = table
            .query()
            .select(Select::columns(&[SRC_ROW_ID_COL, SPLIT_ID_COLUMN]))
            .execute()
            .await?;
        let schema = schema(&["row_id", "position", "split_id"]);
        let writer = Writer::new(&self.disk(), schema.clone())?;
        let mut position = 0u64;
        while let Some(batch) = stream.try_next().await? {
            for offset in (0..batch.num_rows()).step_by(BATCH_ROWS) {
                let length = BATCH_ROWS.min(batch.num_rows() - offset);
                let end = position
                    .checked_add(length as u64)
                    .ok_or_else(|| fail("output position overflow"))?;
                writer
                    .write(RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            batch.column(0).slice(offset, length),
                            Arc::new(UInt64Array::from_iter_values(position..end)),
                            batch.column(1).slice(offset, length),
                        ],
                    )?)
                    .await?;
                position = end;
            }
        }
        writer.finish().await
    }

    async fn merge(
        &self,
        source: Spool,
        output: Spool,
        eligible: u64,
        emitted: u64,
    ) -> Result<Spool> {
        let mut source = Cursor::<2>::new(source);
        let mut output = Cursor::<3>::new(output);
        let schema = schema(&["position", "split_id", "ordinal"]);
        let writer = Writer::new(&self.disk(), schema.clone())?;
        let mut buffer = [
            Vec::with_capacity(BATCH_ROWS),
            Vec::with_capacity(BATCH_ROWS),
            Vec::with_capacity(BATCH_ROWS),
        ];
        let mut current: Option<[u64; 2]> = None;
        let mut previous_source = None;
        let mut source_count = 0u64;
        let mut output_count = 0u64;
        let mut previous_output = None;
        while let Some([row_id, position, split]) = output.next().await? {
            if position >= emitted || previous_output.is_some_and(|key| key >= (row_id, position)) {
                return Err(fail("invalid sorted output sidecar order/position"));
            }
            previous_output = Some((row_id, position));
            while current.is_none_or(|[id, _]| id < row_id) {
                match source.next().await? {
                    Some([id, ordinal]) => {
                        if ordinal >= eligible
                            || previous_source.is_some_and(|previous| previous >= id)
                        {
                            return Err(fail("duplicate source row ID or invalid ordinal"));
                        }
                        previous_source = Some(id);
                        source_count += 1;
                        current = Some([id, ordinal]);
                    }
                    None => return Err(fail("output occurrence missing from eligible source")),
                }
            }
            let [id, ordinal] = current.unwrap();
            if id != row_id {
                return Err(fail("output occurrence missing from eligible source"));
            }
            for (column, value) in buffer.iter_mut().zip([position, split, ordinal]) {
                column.push(value);
            }
            output_count += 1;
            if buffer[0].len() == BATCH_ROWS {
                write_buffer(&writer, &schema, &mut buffer).await?;
            }
        }
        // Check all source rows, including unused discarded tails.
        while let Some([id, ordinal]) = source.next().await? {
            if ordinal >= eligible || previous_source.is_some_and(|previous| previous >= id) {
                return Err(fail("duplicate source row ID or invalid ordinal"));
            }
            previous_source = Some(id);
            source_count += 1;
        }
        if source_count != eligible || output_count != emitted {
            return Err(fail("sidecar merge cardinality mismatch"));
        }
        if !buffer[0].is_empty() {
            write_buffer(&writer, &schema, &mut buffer).await?;
        }
        writer.finish().await
    }

    pub async fn finish(
        &self,
        table: &Table,
        completion: Completion,
        eligible: u64,
    ) -> Result<String> {
        let source = completion
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| fail("source sidecar did not reach checked EOF"))?;
        if source.rows != eligible {
            return Err(fail("eligible source count changed"));
        }
        let output = self.final_positions(table).await?;
        let emitted = output.rows;
        let source = self.sort(source, &["row_id"]).await?;
        let output = self.sort(output, &["row_id", "position"]).await?;
        let resolved = self.merge(source, output, eligible, emitted).await?;
        let ordered = self.sort(resolved, &["position"]).await?;
        let mut rows = Cursor::<3>::new(ordered);
        let mut hash = Sha256::new();
        hash.update(DOMAIN);
        hash.update(eligible.to_le_bytes());
        let mut position = 0u64;
        while let Some([actual, split, ordinal]) = rows.next().await? {
            if actual != position || ordinal >= eligible {
                return Err(fail("sidecar output positions not contiguous"));
            }
            hash.update(split.to_le_bytes());
            hash.update(ordinal.to_le_bytes());
            position += 1;
        }
        if position != emitted {
            return Err(fail("sidecar final cardinality mismatch"));
        }
        hash.update(emitted.to_le_bytes());
        Ok(format!("logical-occurrences-v1:{:x}", hash.finalize()))
    }
}
async fn write_buffer(
    writer: &Writer,
    schema: &SchemaRef,
    buffer: &mut [Vec<u64>; 3],
) -> Result<()> {
    let arrays = buffer
        .iter_mut()
        .map(|column| {
            Arc::new(UInt64Array::from(std::mem::replace(
                column,
                Vec::with_capacity(BATCH_ROWS),
            ))) as arrow_array::ArrayRef
        })
        .collect();
    writer
        .write(RecordBatch::try_new(schema.clone(), arrays)?)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connect;

    fn input(ids: &[Vec<u64>]) -> SendableRecordBatchStream {
        let schema = schema(&[ROW_ID]);
        let batches = ids
            .iter()
            .map(|ids| {
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(ids.clone()))],
                )
                .unwrap())
            })
            .collect::<Vec<_>>();
        Box::pin(SimpleRecordBatchStream {
            schema,
            stream: futures::stream::iter(batches),
        })
    }
    async fn table(rows: &[(u64, u64)]) -> Table {
        let db = connect("memory:///").execute().await.unwrap();
        let schema = schema(&[SRC_ROW_ID_COL, SPLIT_ID_COLUMN]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|r| r.1))),
            ],
        )
        .unwrap();
        let stream = SimpleRecordBatchStream {
            schema,
            stream: futures::stream::iter(vec![Ok(batch)]),
        };
        db.create_table("plan", Box::pin(stream) as SendableRecordBatchStream)
            .execute()
            .await
            .unwrap()
    }
    fn correlation(temp: &TemporaryDirectory) -> Correlation {
        Correlation::new(temp, 16 * 1024 * 1024, 256 * 1024 * 1024).unwrap()
    }

    #[test_log::test(tokio::test)]
    async fn sidecar_exact_batches_and_duplicate_output() {
        let engine = correlation(&TemporaryDirectory::OsDefault);
        let expected = input(&[vec![10], vec![20, 30]])
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let original = Box::pin(SimpleRecordBatchStream {
            schema: expected[0].schema(),
            stream: futures::stream::iter(expected.clone().into_iter().map(Ok)),
        });
        let (mut tapped, receipt) = engine.tap(original).unwrap();
        assert!(receipt.lock().unwrap().is_none());
        for expected in &expected {
            let actual = tapped.try_next().await.unwrap().unwrap();
            assert_eq!(&actual, expected);
            assert!(Arc::ptr_eq(actual.column(0), expected.column(0)));
            assert!(Arc::ptr_eq(&actual.schema(), &expected.schema()));
        }
        assert!(tapped.try_next().await.unwrap().is_none());
        let plan = table(&[(30, 0), (10, 0), (30, 1), (20, 1)]).await;
        let actual = engine.finish(&plan, receipt, 3).await.unwrap();
        // Fixed golden encoded independently with Python struct <Q/<QQ.
        assert_eq!(
            actual,
            "logical-occurrences-v1:c50d9ba8f87e31c1509ed7046b03675a95136d2a9d2bcdf45f97b56e2939769d"
        );
        assert_eq!(engine.disk().used_disk_space(), 0);
    }

    #[tokio::test]
    async fn sidecar_empty_and_bad_source() {
        for (ids, rows, succeeds) in [
            (vec![], vec![], true),
            (vec![10, 20], vec![(99, 0)], false),
            (vec![10, 20, 20], vec![(10, 0)], false),
        ] {
            let engine = correlation(&TemporaryDirectory::OsDefault);
            let (stream, receipt) = engine.tap(input(std::slice::from_ref(&ids))).unwrap();
            stream.try_collect::<Vec<_>>().await.unwrap();
            assert_eq!(
                engine
                    .finish(&table(&rows).await, receipt, ids.len() as u64)
                    .await
                    .is_ok(),
                succeeds
            );
        }
    }

    #[tokio::test]
    async fn sidecar_cancel_and_temp_none() {
        assert!(Correlation::new(&TemporaryDirectory::None, 16 * 1024 * 1024, 1024).is_err());
        let directory = tempfile::tempdir().unwrap();
        let engine = correlation(&TemporaryDirectory::Specific(directory.path().into()));
        let (mut stream, receipt) = engine.tap(input(&[vec![1], vec![2]])).unwrap();
        stream.try_next().await.unwrap();
        // Establish real actor activity before cancellation, not an initial zero.
        for _ in 0..100 {
            if engine.disk().used_disk_space() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(engine.disk().used_disk_space() > 0);
        drop(stream);
        assert!(receipt.lock().unwrap().is_none());
        // Actor IO may already be in flight; cleanup is eventual, not synchronous.
        for _ in 0..100 {
            if engine.disk().used_disk_space() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(engine.disk().used_disk_space(), 0);
        // Also directly await the writer actor after cancellation; accounting alone
        // does not establish actor completion.
        let writer = Writer::new(&engine.disk(), schema(&[ROW_ID])).unwrap();
        let Writer { sender, task } = writer;
        drop(sender);
        assert!(task.await.unwrap().is_err());
        fn files(path: &std::path::Path) -> usize {
            std::fs::read_dir(path)
                .unwrap()
                .map(|e| {
                    let p = e.unwrap().path();
                    if p.is_dir() { files(&p) } else { 1 }
                })
                .sum()
        }
        for _ in 0..100 {
            if files(directory.path()) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(files(directory.path()), 0);
    }

    #[tokio::test]
    async fn sidecar_disk_limit_and_truncated_receipt() {
        let engine = Correlation::new(&TemporaryDirectory::OsDefault, 16 * 1024 * 1024, 1).unwrap();
        let (stream, completion) = engine.tap(input(&[vec![1]])).unwrap();
        assert!(stream.try_collect::<Vec<_>>().await.is_err());
        assert!(completion.lock().unwrap().is_none());
        // Failed accounting poisons this private runtime; discard, do not reuse.
        drop(engine);
        let engine = correlation(&TemporaryDirectory::OsDefault);
        let (stream, receipt) = engine.tap(input(&[vec![1, 2]])).unwrap();
        stream.try_collect::<Vec<_>>().await.unwrap();
        let spool = receipt.lock().unwrap().take().unwrap();
        spool
            .file
            .inner()
            .as_file()
            .set_len(spool.bytes - 8)
            .unwrap();
        assert!(spool.read().try_collect::<Vec<_>>().await.is_err());
    }
    async fn authored_spool(engine: &Correlation, names: &[&str], rows: &[Vec<u64>]) -> Spool {
        let schema = schema(names);
        let writer = Writer::new(&engine.disk(), schema.clone()).unwrap();
        for row in rows {
            writer
                .write(
                    RecordBatch::try_new(
                        schema.clone(),
                        row.iter()
                            .map(|v| Arc::new(UInt64Array::from(vec![*v])) as arrow_array::ArrayRef)
                            .collect(),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        writer.finish().await.unwrap()
    }

    #[tokio::test]
    async fn sidecar_source_error_never_completes() {
        let engine = correlation(&TemporaryDirectory::OsDefault);
        let schema = schema(&[ROW_ID]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![1]))])
                .unwrap();
        let input = Box::pin(SimpleRecordBatchStream {
            schema,
            stream: futures::stream::iter(vec![Ok(batch), Err(fail("authored source failure"))]),
        });
        let (stream, receipt) = engine.tap(input).unwrap();
        let error = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(error.to_string().contains("authored source failure"));
        assert!(receipt.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn sidecar_rejects_malformed_output_positions() {
        for output in [vec![vec![10, 2, 0]], vec![vec![10, 0, 0], vec![10, 0, 1]]] {
            let engine = correlation(&TemporaryDirectory::OsDefault);
            let source = authored_spool(&engine, &["row_id", "ordinal"], &[vec![10, 0]]).await;
            let emitted = output.len() as u64;
            let output =
                authored_spool(&engine, &["row_id", "position", "split_id"], &output).await;
            assert!(engine.merge(source, output, 1, emitted).await.is_err());
        }
    }

    #[tokio::test]
    async fn sidecar_reader_cancel_and_failed_writer_delete_files() {
        fn files(path: &std::path::Path) -> usize {
            std::fs::read_dir(path)
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok())
                .map(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        files(&p)
                    } else if p.exists() {
                        1
                    } else {
                        0
                    }
                })
                .sum()
        }
        let directory = tempfile::tempdir().unwrap();
        let temp = TemporaryDirectory::Specific(directory.path().into());
        let engine = correlation(&temp);
        let spool =
            authored_spool(&engine, &["value"], &[vec![1], vec![2], vec![3], vec![4]]).await;
        assert!(files(directory.path()) > 0);
        let mut reader = spool.read();
        assert!(reader.try_next().await.unwrap().is_some());
        drop(reader);
        for _ in 0..100 {
            if files(directory.path()) == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(files(directory.path()), 0);
        let failed = Correlation::new(&temp, 16 * 1024 * 1024, 1).unwrap();
        let writer = Writer::new(&failed.disk(), schema(&["value"])).unwrap();
        assert!(writer.finish().await.is_err()); // joins the failed actor
        assert_eq!(files(directory.path()), 0);
        // Accounting state may be poisoned: discard it rather than assert zero.
        drop(failed);
    }
}
