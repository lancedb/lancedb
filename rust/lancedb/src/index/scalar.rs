// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Scalar indices are exact indices that are used to quickly satisfy a variety of filters
//! against a column of scalar values.
//!
//! Scalar indices are currently supported on numeric, string, boolean, and temporal columns.
//!
//! A scalar index will help with queries with filters like `x > 10`, `x < 10`, `x = 10`,
//! etc.  Scalar indices can also speed up prefiltering for vector searches.  A single
//! vector search with prefiltering can use both a scalar index and a vector index.

use std::collections::HashSet;
use std::path::PathBuf;

use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use futures::{TryStreamExt, future::BoxFuture};

use crate::query::{QueryExecutionOptions, QueryRequest, Select};
use crate::table::query::{AnyQuery, execute_generic_query};
use crate::utils::resolve_arrow_field_path;
use crate::{Error, Result, Table};

/// A client-side source for custom full-text-search stop words.
///
/// Every source is resolved by the LanceDB client into an owned, stable
/// `Vec<String>` snapshot before index creation. Only that concrete snapshot is
/// passed to a local Lance index builder or serialized in a remote
/// `create_index` request; a remote server never attempts to read a client-local
/// file path.
///
/// Exact empty strings are ignored and exact duplicates are removed while
/// retaining their first occurrence. Values are otherwise preserved verbatim:
/// LanceDB does not trim, lowercase, or otherwise normalize them.
///
/// A native query using an active custom snapshot (`remove_stop_words=true`)
/// rejects explicit fuzzy matching (`fuzziness > 0`) because Lance's current
/// fuzzy path does not use the persisted tokenizer. Remote tables reject all
/// explicit fuzzy matching until the server protocol can atomically bind a
/// query to a tokenizer snapshot. Exact matching (`fuzziness=0`) and an unset
/// fuzziness remain supported.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum FtsStopWordsSource {
    /// Stop words supplied directly by the caller.
    Inline(Vec<String>),
    /// A UTF-8, newline-delimited file read by the client.
    File(PathBuf),
    /// All values from a local/native table string column; `NULL` is rejected.
    ///
    /// Remote table sources are rejected because a remote query may impose a
    /// result limit and cannot currently prove that the returned snapshot is
    /// complete. Native sources are read directly from one local dataset
    /// snapshot, including MemWAL rows, without Namespace query pushdown. A
    /// remote target index may still use a local table source.
    Table {
        /// The LanceDB table containing the stop words.
        table: Table,
        /// The `Utf8`, `LargeUtf8`, or `Utf8View` column to read.
        column: String,
    },
}

impl FtsStopWordsSource {
    /// Create an inline stop-word source.
    pub fn inline(words: Vec<String>) -> Self {
        Self::Inline(words)
    }

    /// Create a UTF-8, newline-delimited file source.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File(path.into())
    }

    /// Create a LanceDB table-column source.
    ///
    /// The column is projected and streamed instead of materializing the rest
    /// of the table. `NULL` values fail closed because silently dropping them
    /// could produce an unintended tokenizer configuration. The source table
    /// must be local/native; materialize remote stop words locally first.
    pub fn table(table: Table, column: impl Into<String>) -> Self {
        Self::Table {
            table,
            column: column.into(),
        }
    }

    /// Resolve this source into the exact snapshot persisted with an FTS index.
    ///
    /// This method always runs on the client. It is also useful for standalone
    /// tokenization APIs that need to use the same stop-word source semantics as
    /// index creation.
    pub fn resolve(self) -> BoxFuture<'static, Result<Vec<String>>> {
        Box::pin(async move {
            match self {
                Self::Inline(words) => Ok(normalize_stop_words(words)),
                Self::File(path) => {
                    if path.as_os_str().is_empty() {
                        return Err(Error::InvalidInput {
                            message: "custom stop words file source requires a non-empty path"
                                .to_string(),
                        });
                    }
                    let bytes = tokio::fs::read(&path)
                        .await
                        .map_err(|e| Error::InvalidInput {
                            message: format!(
                                "failed to read custom stop words file `{}`: {}",
                                path.display(),
                                e
                            ),
                        })?;
                    let contents = String::from_utf8(bytes).map_err(|e| Error::InvalidInput {
                        message: format!(
                            "custom stop words file `{}` is not valid UTF-8: {}",
                            path.display(),
                            e
                        ),
                    })?;
                    Ok(normalize_stop_words(
                        contents.lines().map(ToOwned::to_owned),
                    ))
                }
                Self::Table { table, column } => {
                    if column.is_empty() {
                        return Err(Error::InvalidInput {
                            message: "custom stop words table source requires a non-empty column"
                                .to_string(),
                        });
                    }
                    resolve_table_stop_words(table, column).await
                }
            }
        })
    }
}

impl From<Vec<String>> for FtsStopWordsSource {
    fn from(words: Vec<String>) -> Self {
        Self::Inline(words)
    }
}

pub(crate) fn normalize_stop_words(words: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    for word in words {
        if !word.is_empty() && seen.insert(word.clone()) {
            normalized.push(word);
        }
    }
    normalized
}

fn append_stop_words<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
    table_name: &str,
    column: &str,
    row_offset: usize,
    stop_words: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<usize> {
    let mut rows = 0;
    for (index, value) in values.into_iter().enumerate() {
        rows += 1;
        let value = value.ok_or_else(|| Error::InvalidInput {
            message: format!(
                "custom stop words column `{}` in table `{}` contains NULL at row {}",
                column,
                table_name,
                row_offset + index
            ),
        })?;
        if !value.is_empty() {
            let value = value.to_owned();
            if seen.insert(value.clone()) {
                stop_words.push(value);
            }
        }
    }
    Ok(rows)
}

async fn resolve_table_stop_words(table: Table, column: String) -> Result<Vec<String>> {
    let table_name = table.name().to_owned();
    let native_table = table.as_native().ok_or_else(|| {
        Error::InvalidInput {
            message: format!(
                "custom stop words table `{}` is remote; remote table sources cannot guarantee a complete snapshot. Materialize the stop-word column into a local LanceDB table, or use an inline list or UTF-8 file",
                table_name
            ),
        }
    })?;
    let schema = table.schema().await.map_err(|e| Error::InvalidInput {
        message: format!(
            "failed to read schema for custom stop words table `{}`: {}",
            table_name, e
        ),
    })?;
    let (canonical_column, field) =
        resolve_arrow_field_path(schema.as_ref(), &column).map_err(|e| Error::InvalidInput {
            message: format!(
                "failed to resolve custom stop words column `{}` in table `{}`: {}",
                column, table_name, e
            ),
        })?;
    if !matches!(
        field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Err(Error::InvalidInput {
            message: format!(
                "custom stop words column `{}` in table `{}` must have type Utf8, LargeUtf8, or Utf8View, but has {}",
                canonical_column,
                table_name,
                field.data_type()
            ),
        });
    }

    let query = AnyQuery::Query(QueryRequest {
        select: Select::columns(&[canonical_column.as_str()]),
        ..Default::default()
    });
    // Do not call `Table::query` here. A native table may have Namespace
    // QueryTable pushdown enabled, whose protocol defaults an unbounded plain
    // query to `k=10`. Reading through the generic local path both avoids that
    // truncation and retains `create_plan`'s automatic MemWAL routing.
    let mut stream = execute_generic_query(native_table, &query, QueryExecutionOptions::default())
        .await
        .map_err(|e| Error::InvalidInput {
            message: format!(
                "failed to query custom stop words column `{}` in table `{}`: {}",
                canonical_column, table_name, e
            ),
        })?;
    let mut stop_words = Vec::new();
    let mut seen = HashSet::new();
    let mut row_offset = 0;
    while let Some(batch) = stream.try_next().await.map_err(|e| Error::InvalidInput {
        message: format!(
            "failed while reading custom stop words column `{}` in table `{}`: {}",
            canonical_column, table_name, e
        ),
    })? {
        if batch.num_columns() != 1 {
            return Err(Error::InvalidInput {
                message: format!(
                    "querying custom stop words column `{}` in table `{}` returned {} columns instead of one",
                    canonical_column,
                    table_name,
                    batch.num_columns()
                ),
            });
        }
        let values = batch.column(0);
        let rows = match values.data_type() {
            DataType::Utf8 => append_stop_words(
                values.as_ref().as_string::<i32>().iter(),
                &table_name,
                &canonical_column,
                row_offset,
                &mut stop_words,
                &mut seen,
            )?,
            DataType::LargeUtf8 => append_stop_words(
                values.as_ref().as_string::<i64>().iter(),
                &table_name,
                &canonical_column,
                row_offset,
                &mut stop_words,
                &mut seen,
            )?,
            DataType::Utf8View => append_stop_words(
                values.as_ref().as_string_view().iter(),
                &table_name,
                &canonical_column,
                row_offset,
                &mut stop_words,
                &mut seen,
            )?,
            data_type => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "querying custom stop words column `{}` in table `{}` returned unexpected type {}",
                        canonical_column, table_name, data_type
                    ),
                });
            }
        };
        row_offset += rows;
    }
    Ok(stop_words)
}

/// Builder for a btree index
///
/// A btree index is an index on scalar columns.  The index stores a copy of the column
/// in sorted order.  A header entry is created for each block of rows (currently the
/// block size is fixed at 4096).  These header entries are stored in a separate
/// cacheable structure (a btree).  To search for data the header is used to determine
/// which blocks need to be read from disk.
///
/// For example, a btree index in a table with 1Bi rows requires sizeof(Scalar) * 256Ki
/// bytes of memory and will generally need to read sizeof(Scalar) * 4096 bytes to find
/// the correct row ids.
///
/// This index is good for scalar columns with mostly distinct values and does best when
/// the query is highly selective.
///
/// The btree index does not currently have any parameters though parameters such as the
/// block size may be added in the future.
#[derive(Default, Debug, Clone, serde::Serialize)]
pub struct BTreeIndexBuilder {}

impl BTreeIndexBuilder {}

/// Builder for a Bitmap index.
///
/// It is a scalar index that stores a bitmap for each possible value
///
/// This index works best for low-cardinality (i.e., less than 1000 unique values) columns,
/// where the number of unique values is small.
/// The bitmap stores a list of row ids where the value is present.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BitmapIndexBuilder {}

/// Builder for LabelList index.
///
/// [LabeListIndexBuilder] is a scalar index that can be used on `List<T>` columns to
/// support queries with `array_contains_all` and `array_contains_any`
/// using an underlying bitmap index.
///
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LabelListIndexBuilder {}

/// Builder for an FM-Index.
///
/// An FM-Index (Ferragina–Manzini) is a scalar index over string/binary columns
/// that accelerates substring search, i.e. `contains(col, 'needle')`. Unlike an
/// inverted (FTS) index it matches arbitrary substrings of the raw bytes rather
/// than tokenized words.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct FmIndexBuilder {}

pub use lance_index::scalar::FullTextSearchQuery;
pub use lance_index::scalar::InvertedIndexParams as FtsIndexBuilder;
pub use lance_index::scalar::InvertedIndexParams;
pub use lance_index::scalar::inverted::query::*;

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow_array::{
        ArrayRef, Int32Array, LargeStringArray, RecordBatch, RecordBatchIterator, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use lance_namespace::LanceNamespace;
    use lance_namespace::models::QueryTableRequest;
    use tempfile::tempdir;

    use super::FtsStopWordsSource;
    use crate::connection::NamespaceClientPushdownOperation;
    use crate::table::BaseTable;
    use crate::{Table, connect};

    #[derive(Debug, Default)]
    struct CountingNamespaceClient {
        query_table_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl LanceNamespace for CountingNamespaceClient {
        fn namespace_id(&self) -> String {
            "counting-stop-words".to_string()
        }

        async fn query_table(&self, _request: QueryTableRequest) -> lance::Result<bytes::Bytes> {
            self.query_table_calls.fetch_add(1, Ordering::SeqCst);
            panic!("custom stop words must be read from the local dataset snapshot");
        }
    }

    #[tokio::test]
    async fn resolve_inline_and_file_stop_words() {
        let inline = FtsStopWordsSource::inline(vec![
            "cat".to_string(),
            String::new(),
            " cat ".to_string(),
            "cat".to_string(),
            "CAT".to_string(),
        ])
        .resolve()
        .await
        .unwrap();
        assert_eq!(inline, vec!["cat", " cat ", "CAT"]);

        let dir = tempdir().unwrap();
        let path = dir.path().join("stop-words.txt");
        std::fs::write(&path, b"cat\r\n\r\n cat \ncat\nCAT\n").unwrap();
        let from_file = FtsStopWordsSource::file(&path).resolve().await.unwrap();
        assert_eq!(from_file, inline);

        let empty_path = dir.path().join("empty.txt");
        std::fs::write(&empty_path, b"\n\r\n").unwrap();
        assert!(
            FtsStopWordsSource::file(&empty_path)
                .resolve()
                .await
                .unwrap()
                .is_empty()
        );

        let invalid_path = dir.path().join("invalid.txt");
        std::fs::write(&invalid_path, [0xff, 0xfe]).unwrap();
        let error = FtsStopWordsSource::file(&invalid_path)
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is not valid UTF-8"));

        let error = FtsStopWordsSource::file(dir.path().join("missing.txt"))
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("failed to read"));
        assert!(error.to_string().contains("missing.txt"));

        let error = FtsStopWordsSource::file("").resolve().await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("file source requires a non-empty path")
        );
    }

    #[tokio::test]
    async fn resolve_table_stop_words_and_validate_column() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("word", DataType::Utf8, false),
                Field::new("large_word", DataType::LargeUtf8, false),
                Field::new("number", DataType::Int32, false),
                Field::new("nullable_word", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["cat", "", "cat", " CAT "])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec!["large", "large", "", "other"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec![
                    Some("valid"),
                    None,
                    Some("later"),
                    Some("last"),
                ])),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("stop_words", batch)
            .execute()
            .await
            .unwrap();

        assert_eq!(
            FtsStopWordsSource::table(table.clone(), "word")
                .resolve()
                .await
                .unwrap(),
            vec!["cat", " CAT "]
        );
        assert_eq!(
            FtsStopWordsSource::table(table.clone(), "large_word")
                .resolve()
                .await
                .unwrap(),
            vec!["large", "other"]
        );

        let error = FtsStopWordsSource::table(table.clone(), "nullable_word")
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("contains NULL at row 1"));

        let error = FtsStopWordsSource::table(table.clone(), "number")
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("must have type Utf8"));
        assert!(error.to_string().contains("Int32"));

        let error = FtsStopWordsSource::table(table, "missing")
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("failed to resolve"));
        assert!(error.to_string().contains("missing"));

        let error =
            FtsStopWordsSource::table(conn.open_table("stop_words").execute().await.unwrap(), "")
                .resolve()
                .await
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("table source requires a non-empty column")
        );
    }

    #[cfg(feature = "remote")]
    #[tokio::test]
    async fn reject_remote_table_stop_words_source() {
        let table =
            crate::Table::new_with_handler("remote_stop_words", |_| -> http::Response<String> {
                panic!("remote source validation should fail before issuing a request")
            });
        let error = FtsStopWordsSource::table(table, "word")
            .resolve()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("is remote"));
        assert!(
            error
                .to_string()
                .contains("cannot guarantee a complete snapshot")
        );
        assert!(error.to_string().contains("local LanceDB table"));
    }

    #[tokio::test]
    async fn table_stop_words_bypass_namespace_query_limits() {
        let conn = connect("memory://").execute().await.unwrap();
        let words = (0..12)
            .map(|index| format!("word-{index}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("word", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from_iter_values(
                words.iter().map(String::as_str),
            ))],
        )
        .unwrap();
        let table = conn
            .create_table("stop_words_namespace_source", batch)
            .execute()
            .await
            .unwrap();
        let namespace_client = Arc::new(CountingNamespaceClient::default());
        let mut native_table = table.as_native().unwrap().clone();
        native_table.namespace_client = Some(namespace_client.clone());
        native_table
            .pushdown_operations
            .insert(NamespaceClientPushdownOperation::QueryTable);
        let source_table = Table::from(Arc::new(native_table) as Arc<dyn BaseTable>);

        assert_eq!(
            FtsStopWordsSource::table(source_table, "word")
                .resolve()
                .await
                .unwrap(),
            words
        );
        assert_eq!(namespace_client.query_table_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn table_stop_words_include_uncompacted_memwal_rows() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("word", DataType::Utf8, false),
        ]));
        let initial_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["base-one", "base-two"])),
            ],
        )
        .unwrap();
        let table = conn
            .create_table("stop_words_memwal_source", initial_batch)
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(crate::table::LsmWriteSpec::unsharded())
            .await
            .unwrap();

        let memwal_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(StringArray::from(vec!["memwal"])),
            ],
        )
        .unwrap();
        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(memwal_batch)],
                schema,
            )))
            .await
            .unwrap();

        let mut words = FtsStopWordsSource::table(table, "word")
            .resolve()
            .await
            .unwrap();
        words.sort();
        assert_eq!(words, vec!["base-one", "base-two", "memwal"]);
    }
}
