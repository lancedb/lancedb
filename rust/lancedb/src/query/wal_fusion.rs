// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! WAL-PK-FUSION: hybrid search's primary-key fallback for MemWAL tables.
//!
//! Hybrid search joins its vector and full-text legs on `_rowid`, which a
//! MemWAL table cannot supply: the fresh tier has no stable row id, so the
//! server rejects `with_row_id` before it plans. When that happens the legs are
//! joined on a surrogate `_rowid` built from the table's primary key instead.
//!
//! This is a stopgap until MemWAL supports `_rowid`, and everything it needs
//! lives here so it can be removed in one pass:
//!
//! 1. Delete this file and its `mod wal_fusion;` line in `query.rs`.
//! 2. Run `grep -rn WAL-PK-FUSION rust/ python/` and follow the note at each
//!    marker; the Python side has its own copy in `lancedb/_wal_hybrid.py`.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::row::{RowConverter, SortField};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field};
use lance::dataset::ROW_ID;
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::{LANCE_UNENFORCED_PRIMARY_KEY, LANCE_UNENFORCED_PRIMARY_KEY_POSITION};
use lance_core::utils::parse::str_is_truthy;

use super::{Select, VectorQuery};
use crate::error::{Error, Result};

/// The part of the MemWAL `with_row_id` refusal that identifies it.
///
/// Both layers raise this same sentence verbatim — sophon's
/// `LSM_WITH_ROW_ID_UNSUPPORTED` says so in as many words, and the OSS LSM
/// scanner builds it from the same words — so this substring survives either
/// path and the wrapping each adds. It is a coupling to a message rather than
/// a code, because the refusal arrives as a generic 400; if the server ever
/// rewords it the fallback stops firing and hybrid on a MemWAL table goes back
/// to erroring visibly, which is loud rather than silently wrong.
const WAL_ROW_ID_REFUSAL: &str = "does not support with_row_id";

/// Whether `error` is the server declining `_rowid` because the table is
/// MemWAL-backed, as opposed to any other failure the query might hit.
fn is_wal_row_id_refusal(error: &Error) -> bool {
    error.to_string().contains(WAL_ROW_ID_REFUSAL)
}

/// Why a hybrid query on a MemWAL table cannot hand back `_rowid`, raised
/// whether the refusal was anticipated or came back from the server.
fn wal_row_id_unsupported() -> Error {
    Error::NotSupported {
        message: "hybrid search on a MemWAL table cannot return _rowid: the fresh tier has no \
                  stable row id, and the ids the fusion joins on are synthesized from the \
                  primary key. Set use_lsm(false) to read the base table only (results will \
                  exclude un-compacted MemWAL data)"
            .to_string(),
    }
}

/// Run a hybrid query, joining its legs on the primary key instead of `_rowid`
/// when the table is MemWAL-backed.
///
/// `run(None)` must join the legs on `_rowid`; `run(Some(fusion))` must build
/// them with `fusion` instead.
///
/// `_rowid` is asked for first and the primary key used only when the server
/// refuses, rather than paying a round trip up front to find out. Only a
/// MemWAL table refuses, so probing would tax every table to learn something
/// almost none of them need — and the refusal has to be handled regardless,
/// since a write spec installed elsewhere can arrive between any two queries.
/// The refusal costs nothing server-side: it is raised before the query is
/// planned.
pub(super) async fn with_pk_fallback<T, F, Fut>(query: &VectorQuery, run: F) -> Result<T>
where
    F: Fn(Option<PkFusion>) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // A previous query on this table was already refused, so skip straight to
    // the key.
    if query.request.base.use_lsm != Some(false) && query.parent.hybrid_pk_fusion_learned() {
        return run(Some(PkFusion::resolve(query).await?)).await;
    }
    match run(None).await {
        Err(e) if is_wal_row_id_refusal(&e) => {
            // A caller who asked for `_rowid` gets the reason, not a bare 400
            // — the same message the learned path raises up front.
            if query.request.base.with_row_id {
                return Err(wal_row_id_unsupported());
            }
            // Learned, so later queries on this table skip straight to it.
            query.parent.note_hybrid_pk_fusion();
            run(Some(PkFusion::resolve(query).await?)).await
        }
        other => other,
    }
}

/// How long a table remembers that it refused `_rowid`.
///
/// Nothing fetches this — the refusal seeds it — so the bound exists only to
/// stop the memory outliving a write spec someone removed through another
/// handle. Being late to notice keeps the query on the primary key, which on a
/// base table is still a valid read; the window is what stops that lasting.
const MEMORY_TTL: Duration = Duration::from_secs(30);

/// A table's memory of having refused `_rowid`, backing
/// [`BaseTable::hybrid_pk_fusion_learned`](crate::table::BaseTable::hybrid_pk_fusion_learned)
/// for table types that keep one.
#[derive(Debug, Default)]
pub struct PkFusionMemory(Mutex<Option<Instant>>);

impl PkFusionMemory {
    pub fn learned(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .is_some_and(|learned| learned.elapsed() < MEMORY_TTL)
    }

    pub fn note(&self) {
        *self.0.lock().unwrap() = Some(Instant::now());
    }

    /// Forget the refusal, as when the write spec is removed through this handle.
    pub fn forget(&self) {
        *self.0.lock().unwrap() = None;
    }
}

/// How the legs of a hybrid query on a MemWAL table are joined: on a surrogate
/// `_rowid` derived client-side from the primary key.
pub(super) struct PkFusion {
    columns: Vec<String>,
    /// Key columns the caller did not ask for, added to the projection so the
    /// surrogate can be built and dropped again before returning.
    injected: Vec<String>,
}

impl PkFusion {
    /// Resolve the primary key the legs will be fused on. Needs the schema,
    /// which the table has already fetched and cached for its own reasons.
    async fn resolve(query: &VectorQuery) -> Result<Self> {
        if query.request.base.with_row_id {
            return Err(wal_row_id_unsupported());
        }
        let schema = query.parent.schema().await?;
        let columns = pk_columns(&schema);
        let injected = pk_columns_to_inject(&query.request.base.select, &columns);
        Ok(Self { columns, injected })
    }

    /// Make a leg return the key columns, in place of the `_rowid` a MemWAL
    /// table would refuse.
    pub(super) fn prepare_leg(&self, select: &mut Select) {
        inject_pk_columns(select, &self.columns);
    }

    /// Give both legs a surrogate `_rowid` built from the key.
    pub(super) fn stamp(
        &self,
        vector: RecordBatch,
        fts: RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch)> {
        // Vector first: `merge_results` concatenates in that order and keeps
        // the first occurrence, so first-seen ids preserve its tie break.
        let mut legs = [vector, fts];
        stamp_surrogate_row_ids(&mut legs, &self.columns)?;
        let [vector, fts] = legs;
        Ok((vector, fts))
    }

    /// Drop the surrogate and any injected key columns from the reranked
    /// results. The surrogate is an internal join key, never an answer: it
    /// goes whether or not the caller asked for `_rowid`, and `resolve` has
    /// already refused the query if they did.
    pub(super) fn strip(&self, results: RecordBatch) -> Result<RecordBatch> {
        let mut results = results.drop_column(ROW_ID)?;
        for column in &self.injected {
            // A reranker is free to reshape its output, so only drop what
            // actually came back.
            if results.schema().column_with_name(column).is_some() {
                results = results.drop_column(column)?;
            }
        }
        Ok(results)
    }
}

/// The key columns `select` does not already produce, and so would have to be
/// added to it for the surrogate to be buildable.
fn pk_columns_to_inject(select: &Select, pk_columns: &[String]) -> Vec<String> {
    let produced: Vec<&String> = match select {
        // Already every non-system column, the key among them.
        Select::All => return Vec::new(),
        Select::Columns(columns) => columns.iter().collect(),
        Select::Dynamic(pairs) => pairs.iter().map(|(name, _)| name).collect(),
        Select::Expr(pairs) => pairs.iter().map(|(name, _)| name).collect(),
    };
    pk_columns
        .iter()
        .filter(|pk| !produced.contains(pk))
        .cloned()
        .collect()
}

/// Add the key columns `select` is missing, as identity projections where the
/// selection is expression-shaped.
fn inject_pk_columns(select: &mut Select, pk_columns: &[String]) {
    let missing = pk_columns_to_inject(select, pk_columns);
    if missing.is_empty() {
        return;
    }
    match select {
        Select::All => {}
        Select::Columns(columns) => columns.extend(missing),
        Select::Dynamic(pairs) => pairs.extend(missing.into_iter().map(|pk| (pk.clone(), pk))),
        Select::Expr(pairs) => {
            pairs.extend(missing.into_iter().map(|pk| {
                let expr = crate::expr::col(&pk);
                (pk, expr)
            }));
        }
    }
}

/// The unenforced primary key columns declared in `schema`, in key order.
///
/// Reads the field metadata directly rather than going through
/// [`lance_core::datatypes::Schema::unenforced_primary_key`], because a remote
/// table only ever has the Arrow schema `describe` returned — there is no Lance
/// schema, and no field ids, on that side of the wire.
///
/// Ordering mirrors Lance's: fields with an explicit 1-based position first, in
/// position order, then fields carrying only the legacy boolean marker, in
/// declaration order. Only top-level fields are considered, which is every
/// field a supported key dtype can live on.
fn pk_columns(schema: &arrow_schema::Schema) -> Vec<String> {
    let mut keyed: Vec<(bool, u32, usize, &str)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            let metadata = field.metadata();
            let position = metadata
                .get(LANCE_UNENFORCED_PRIMARY_KEY_POSITION)
                .and_then(|value| value.parse::<u32>().ok())
                .or_else(|| {
                    metadata
                        .get(LANCE_UNENFORCED_PRIMARY_KEY)
                        .filter(|value| str_is_truthy(value))
                        .map(|_| 0)
                })?;
            Some((position == 0, position, idx, field.name().as_str()))
        })
        .collect();
    keyed.sort_unstable();
    keyed
        .into_iter()
        .map(|(_, _, _, name)| name.to_string())
        .collect()
}

/// Replace each batch's join column with a dense surrogate derived from
/// `pk_columns`, in place and in the order given.
///
/// The fusion joins its two legs on [`ROW_ID`], which a MemWAL table cannot
/// supply — the fresh tier has no stable row id. It only ever compares the
/// column for equality, though (`merge_results` dedups, the rerankers group and
/// the score restore looks up), so any injective mapping of the primary key
/// serves. Ids are assigned in first-seen order across `batches`, so passing
/// them in fusion order keeps the dedup's "first occurrence wins" and its tie
/// break byte-identical to what a real row id would have produced.
///
/// Writing it under the [`ROW_ID`] name is what lets every reranker, including
/// third-party implementations that hardcode the name, run unmodified. The
/// column never reaches the caller: the hybrid query drops it before returning.
fn stamp_surrogate_row_ids(batches: &mut [RecordBatch], pk_columns: &[String]) -> Result<()> {
    if pk_columns.is_empty() {
        return Err(Error::InvalidInput {
            message: "hybrid search on a MemWAL table needs an unenforced primary key to \
                      deduplicate on, and this table declares none"
                .to_string(),
        });
    }

    let mut converter: Option<RowConverter> = None;
    let mut ids: HashMap<Vec<u8>, u64> = HashMap::new();

    for batch in batches.iter_mut() {
        // Both legs empty: `query_schemas` synthesizes a schema carrying only
        // the score and join columns, so there is no key to read — and no row
        // that would need one.
        let row_ids: UInt64Array = if batch.num_rows() == 0 {
            UInt64Array::from(Vec::<u64>::new())
        } else {
            let key_columns = pk_columns
                .iter()
                .map(|name| {
                    batch
                        .column_by_name(name)
                        .cloned()
                        .ok_or_else(|| Error::InvalidInput {
                            message: format!(
                                "hybrid search could not deduplicate: primary key column {} \
                                 is missing from a result set with {} rows",
                                name,
                                batch.num_rows()
                            ),
                        })
                })
                .collect::<Result<Vec<_>>>()?;

            let converter = match converter {
                Some(ref converter) => converter,
                None => converter.insert(RowConverter::new(
                    key_columns
                        .iter()
                        .map(|column| SortField::new(column.data_type().clone()))
                        .collect(),
                )?),
            };

            let rows = converter.convert_columns(&key_columns)?;
            let next = &mut ids;
            UInt64Array::from_iter_values(rows.iter().map(|row| {
                let id = next.len() as u64;
                *next.entry(row.as_ref().to_vec()).or_insert(id)
            }))
        };

        // `query_schemas` synthesizes a schema carrying `ROW_ID` when a leg came
        // back empty, so the column can already be there — and it is ours to
        // define either way.
        let base = match batch.schema().column_with_name(ROW_ID) {
            Some(_) => batch.drop_column(ROW_ID)?,
            None => batch.clone(),
        };
        *batch = base.try_with_column(
            Field::new(ROW_ID, DataType::UInt64, false),
            Arc::new(row_ids),
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::downcast_array;
    use arrow_array::{Float32Array, StringArray};
    use arrow_schema::Schema;
    use lance_index::{scalar::inverted::SCORE_COL, vector::DIST_COL};

    use super::super::hybrid::{empty_fts_schema, empty_vec_schema};
    use super::*;

    fn field(name: &str, metadata: &[(&str, &str)]) -> Field {
        Field::new(name, DataType::Utf8, false).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn pk_columns_reads_the_position_marker() {
        let schema = Schema::new(vec![
            field("other", &[]),
            field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")]),
        ]);
        assert_eq!(pk_columns(&schema), vec!["id".to_string()]);
    }

    /// Key order is the position, not the order the columns are declared in.
    #[test]
    fn pk_columns_orders_a_composite_key_by_position() {
        let schema = Schema::new(vec![
            field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "2")]),
            field("tenant", &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")]),
        ]);
        assert_eq!(
            pk_columns(&schema),
            vec!["tenant".to_string(), "id".to_string()]
        );
    }

    /// Datasets written before the position marker carry only the boolean.
    #[test]
    fn pk_columns_accepts_the_legacy_boolean_marker() {
        let schema = Schema::new(vec![field("id", &[(LANCE_UNENFORCED_PRIMARY_KEY, "true")])]);
        assert_eq!(pk_columns(&schema), vec!["id".to_string()]);
    }

    /// Mirrors Lance: an explicit position sorts ahead of a legacy field.
    #[test]
    fn pk_columns_sorts_positioned_fields_ahead_of_legacy_ones() {
        let schema = Schema::new(vec![
            field("legacy", &[(LANCE_UNENFORCED_PRIMARY_KEY, "yes")]),
            field(
                "positioned",
                &[(LANCE_UNENFORCED_PRIMARY_KEY_POSITION, "1")],
            ),
        ]);
        assert_eq!(
            pk_columns(&schema),
            vec!["positioned".to_string(), "legacy".to_string()]
        );
    }

    #[test]
    fn pk_columns_is_empty_without_a_key() {
        let schema = Schema::new(vec![
            field("a", &[]),
            field("b", &[(LANCE_UNENFORCED_PRIMARY_KEY, "false")]),
        ]);
        assert!(pk_columns(&schema).is_empty());
    }

    fn batch(ids: Vec<&str>, score: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Utf8, false)),
            Arc::new(Field::new(score, DataType::Float32, false)),
        ]));
        let scores = Float32Array::from(vec![0.0_f32; ids.len()]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(ids)), Arc::new(scores)],
        )
        .unwrap()
    }

    fn row_ids(batch: &RecordBatch) -> Vec<u64> {
        let ids: UInt64Array = downcast_array(batch.column_by_name(ROW_ID).unwrap());
        ids.values().to_vec()
    }

    /// The same key in both legs has to land on the same id or the fusion will
    /// not dedup, and ids must ascend in fusion order so the tie break holds.
    #[test]
    fn test_surrogate_row_ids_are_shared_across_legs_in_first_seen_order() {
        let mut batches = vec![
            batch(vec!["a", "b", "c"], DIST_COL),
            batch(vec!["b", "d"], SCORE_COL),
        ];
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();

        assert_eq!(row_ids(&batches[0]), vec![0, 1, 2]);
        assert_eq!(
            row_ids(&batches[1]),
            vec![1, 3],
            "b keeps the id the vector leg gave it"
        );
    }

    /// A string key is the common case and cannot be cast to the u64 the
    /// rerankers read, which is the whole reason for the surrogate.
    #[test]
    fn test_surrogate_row_ids_repeat_within_a_leg() {
        let mut batches = vec![batch(vec!["a", "a", "b"], DIST_COL)];
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();
        assert_eq!(row_ids(&batches[0]), vec![0, 0, 1]);
    }

    #[test]
    fn test_surrogate_row_ids_support_a_composite_key() {
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("tenant", DataType::Utf8, false)),
            Arc::new(Field::new("id", DataType::Int32, false)),
        ]));
        let make = |tenants: Vec<&str>, ids: Vec<i32>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(tenants)),
                    Arc::new(arrow_array::Int32Array::from(ids)),
                ],
            )
            .unwrap()
        };
        let mut batches = vec![make(vec!["x", "y"], vec![1, 1]), make(vec!["y"], vec![1])];
        stamp_surrogate_row_ids(&mut batches, &["tenant".to_string(), "id".to_string()]).unwrap();

        assert_eq!(
            row_ids(&batches[0]),
            vec![0, 1],
            "same id, different tenant"
        );
        assert_eq!(row_ids(&batches[1]), vec![1]);
    }

    /// Both legs empty: `query_schemas` synthesizes a schema with no key column,
    /// and there is no row that would need one.
    #[test]
    fn test_surrogate_row_ids_tolerate_the_both_empty_schema() {
        let mut batches = vec![
            RecordBatch::new_empty(Arc::new(empty_vec_schema())),
            RecordBatch::new_empty(Arc::new(empty_fts_schema())),
        ];
        stamp_surrogate_row_ids(&mut batches, &["id".to_string()]).unwrap();
        assert_eq!(row_ids(&batches[0]), Vec::<u64>::new());
    }

    /// Rows with no key would silently fuse into one bucket, so refuse.
    #[test]
    fn test_surrogate_row_ids_reject_a_missing_key_column() {
        let mut batches = vec![batch(vec!["a"], DIST_COL)];
        let err = stamp_surrogate_row_ids(&mut batches, &["nope".to_string()]).unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn test_surrogate_row_ids_reject_a_table_with_no_primary_key() {
        let mut batches = vec![batch(vec!["a"], DIST_COL)];
        let err = stamp_surrogate_row_ids(&mut batches, &[]).unwrap_err();
        assert!(err.to_string().contains("unenforced primary key"), "{err}");
    }
}
