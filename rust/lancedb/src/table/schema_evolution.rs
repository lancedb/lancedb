// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema evolution operations for LanceDB tables.
//!
//! This module provides functionality to modify the schema of existing tables:
//! - [`add_columns`](execute_add_columns): Add new columns using SQL expressions
//! - [`alter_columns`](execute_alter_columns): Rename columns, change types, or modify nullability
//! - [`drop_columns`](execute_drop_columns): Remove columns from the table

use arrow_schema::Schema as ArrowSchema;
use lance::dataset::transaction::{Operation, Transaction, UpdateMap, UpdateMapEntry};
use lance::dataset::{ColumnAlteration, CommitBuilder, Dataset, NewColumnTransform};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::computed_columns;
use super::{BaseTable, NativeTable};
use crate::{Error, Result};

/// The result of an add columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// The result of an alter columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AlterColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// The result of a drop columns operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DropColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// A single field's metadata update, addressed by dot-path.
///
/// Merges into the field's existing metadata by default. Use [`Self::remove`] to
/// delete a key, or [`Self::replace`] to swap the field's entire metadata map.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct FieldMetadataUpdate {
    /// Dot-separated path to the field (e.g. `"embedding"` or `"address.zip"`).
    pub path: String,
    /// Keys to set (`Some`) or delete (`None`). See
    /// [`Table::update_field_metadata`](crate::Table::update_field_metadata) for
    /// the conventional `lancedb:*` keys.
    pub metadata: HashMap<String, Option<String>>,
    /// If `true`, replace the field's entire metadata map instead of merging.
    pub replace: bool,
}

impl FieldMetadataUpdate {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            metadata: HashMap::new(),
            replace: false,
        }
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), Some(value.into()));
        self
    }

    pub fn remove(mut self, key: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), None);
        self
    }

    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateFieldMetadataResult {
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the add columns logic.
///
/// Adds new columns to the table using the provided transforms.
pub(crate) async fn execute_add_columns(
    table: &NativeTable,
    transforms: NewColumnTransform,
    read_columns: Option<Vec<String>>,
) -> Result<AddColumnsResult> {
    computed_columns::ensure_not_function_bound(
        table.schema().await?.as_ref(),
        "schema evolution",
        new_column_names(&transforms),
    )?;
    // Declarations are admitted only through [`execute_declare`].
    match &transforms {
        NewColumnTransform::AllNulls(schema) => {
            computed_columns::ensure_no_foreign_declarations(schema.fields())?
        }
        NewColumnTransform::BatchUDF(udf) => {
            computed_columns::ensure_no_foreign_declarations(udf.output_schema.fields())?
        }
        _ => {}
    }
    commit_add_columns(table, transforms, read_columns).await
}

/// Declare validated computed columns. The only admission path for
/// declaration metadata.
pub(crate) async fn execute_declare(
    table: &NativeTable,
    columns: &[(String, String)],
) -> Result<AddColumnsResult> {
    use lance::dataset::mem_wal::DatasetMemWalExt;

    // An LSM write spec keeps visible rows in tiers refresh cannot reach;
    // checked against latest committed state, not this handle's snapshot.
    table.checkout_latest().await?;
    computed_columns::ensure_not_function_bound(
        table.schema().await?.as_ref(),
        "schema evolution",
        columns.iter().map(|(name, _)| name),
    )?;
    // Unset drops the MemWAL index, so the spec alone stops describing a table
    // whose SSTables still hold rows. The shard directories outlive it and are
    // the durable evidence.
    let retained_sstables = !table
        .dataset
        .get()
        .await?
        .list_mem_wal_latest_shard_ids()
        .await?
        .is_empty();
    if retained_sstables || table.get_lsm_write_spec().await?.is_some() {
        return Err(Error::NotSupported {
            message: "computed columns are not supported on a table with an LSM write \
                      spec: rows in un-compacted tiers are invisible to refresh"
                .into(),
        });
    }
    let transform = computed_columns::declare(table.schema().await?, columns)?;
    commit_add_columns(table, transform, None).await
}

/// The top-level columns `transforms` adds.
pub(crate) fn new_column_names(transforms: &NewColumnTransform) -> Vec<String> {
    let names = |schema: &ArrowSchema| {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>()
    };
    match transforms {
        NewColumnTransform::SqlExpressions(expressions) => {
            expressions.iter().map(|(name, _)| name.clone()).collect()
        }
        NewColumnTransform::AllNulls(schema) => names(schema),
        NewColumnTransform::BatchUDF(udf) => names(&udf.output_schema),
        NewColumnTransform::Stream(stream) => names(&stream.schema()),
        NewColumnTransform::Reader(reader) => names(&reader.schema()),
    }
}

pub(crate) async fn commit_add_columns(
    table: &NativeTable,
    transforms: NewColumnTransform,
    read_columns: Option<Vec<String>>,
) -> Result<AddColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    dataset.add_columns(transforms, read_columns, None).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(AddColumnsResult { version })
}

/// Internal implementation of the alter columns logic.
///
/// Alters existing columns in the table (rename, change type, or modify nullability).
pub(crate) async fn execute_alter_columns(
    table: &NativeTable,
    alterations: &[ColumnAlteration],
) -> Result<AlterColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    let schema = std::sync::Arc::new(ArrowSchema::from(dataset.schema()));
    // A Function binding stores its columns' exact fields, nullability
    // included, so every alteration of one counts, and a rename's target too.
    computed_columns::ensure_not_function_bound(
        schema.as_ref(),
        "schema evolution",
        alterations.iter().flat_map(|alteration| {
            std::iter::once(alteration.path.as_str()).chain(alteration.rename.as_deref())
        }),
    )?;
    // Nullability is not part of what an expression resolves against, so only
    // a rename or a retype can invalidate a binding.
    let rebinding = alterations
        .iter()
        .filter(|alteration| alteration.rename.is_some() || alteration.data_type.is_some())
        .map(|alteration| alteration.path.as_str())
        .collect::<Vec<_>>();
    computed_columns::ensure_not_an_input(&schema, &rebinding)?;
    let retyped = alterations
        .iter()
        .filter(|alteration| alteration.data_type.is_some())
        .map(|alteration| alteration.path.as_str())
        .collect::<Vec<_>>();
    computed_columns::ensure_not_retyped(schema.as_ref(), &retyped)?;
    dataset.alter_columns(alterations).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(AlterColumnsResult { version })
}

/// Internal implementation of the drop columns logic.
///
/// Removes columns from the table.
pub(crate) async fn execute_drop_columns(
    table: &NativeTable,
    columns: &[&str],
) -> Result<DropColumnsResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();
    let schema = std::sync::Arc::new(ArrowSchema::from(dataset.schema()));
    let unbinding = computed_columns::plan_function_unbinding(schema.as_ref(), columns)?;

    let mut names = columns.iter().map(|c| c.to_string()).collect::<Vec<_>>();
    names.extend(unbinding.assignment_columns.iter().cloned());
    let dropped = names.iter().map(String::as_str).collect::<Vec<_>>();
    computed_columns::ensure_not_bound_by(&unbinding.retained, "schema evolution", &dropped)?;
    computed_columns::ensure_not_an_input_of(&schema, &dropped, &dropped)?;

    if !unbinding.is_noop() {
        dataset = commit_function_unbinding(dataset, &unbinding).await?;
    }
    dataset.drop_columns(&dropped).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(DropColumnsResult { version })
}

/// Retire the bindings a drop covers, in the commit before it.
///
/// A binding lives in three places -- the schema-metadata envelope, each
/// output's `computed_column.*` field metadata, and the columns themselves --
/// and readers cross-check the first two, so a state with one of them removed
/// is a table that refuses every write. Clearing both metadata halves in one
/// `UpdateConfig` leaves the outputs as ordinary columns holding their last
/// values: valid on its own, and the drop that follows is then an ordinary
/// drop. Interrupted in between, the columns are still there to be dropped
/// again.
///
/// Not folded into the drop's own `Operation::Project`, though it carries a
/// schema: the transaction proto records `Project` as fields alone, so the
/// metadata edit would survive only in this writer's memory.
async fn commit_function_unbinding(
    dataset: Dataset,
    unbinding: &computed_columns::FunctionUnbinding,
) -> Result<Dataset> {
    let bindings = UpdateMap {
        update_entries: vec![UpdateMapEntry {
            key: computed_columns::FUNCTION_BINDINGS_META_KEY.to_string(),
            value: unbinding.bindings_metadata.clone(),
        }],
        replace: false,
    };
    let mut field_metadata_updates = HashMap::new();
    for column in &unbinding.cleared_columns {
        let field = dataset
            .schema()
            .field(column)
            .ok_or_else(|| Error::InvalidInput {
                message: format!("Function output '{column}' does not exist in the table"),
            })?;
        let cleared = field
            .metadata
            .keys()
            .filter(|key| computed_columns::is_declaration_key(key))
            .map(|key| UpdateMapEntry {
                key: key.clone(),
                value: None,
            })
            .collect::<Vec<_>>();
        field_metadata_updates.insert(
            field.id,
            UpdateMap {
                update_entries: cleared,
                replace: false,
            },
        );
    }
    let transaction = Transaction::new(
        dataset.manifest.version,
        Operation::UpdateConfig {
            config_updates: None,
            table_metadata_updates: None,
            schema_metadata_updates: Some(bindings),
            field_metadata_updates,
        },
        None,
    );
    Ok(CommitBuilder::new(std::sync::Arc::new(dataset))
        .execute(transaction)
        .await?)
}

/// Internal implementation of the update field metadata logic.
///
/// Merges or replaces per-field metadata, addressing fields by dot-path.
pub(crate) async fn execute_update_field_metadata(
    table: &NativeTable,
    updates: &[FieldMetadataUpdate],
) -> Result<UpdateFieldMetadataResult> {
    table.dataset.ensure_mutable()?;
    let mut dataset = (*table.dataset.get().await?).clone();

    // A declaration is validated as a whole at declare time; editing its keys
    // here would bypass that, fabricate one on a plain column, or move a
    // binding out from under a refresh. A replace on a declared column would
    // silently erase it.
    let schema = ArrowSchema::from(dataset.schema());
    computed_columns::ensure_not_function_bound(
        &schema,
        "field metadata update",
        updates.iter().map(|update| update.path.as_str()),
    )?;
    let declared: Vec<String> = computed_columns::computed_columns(&schema)
        .into_iter()
        .map(|declaration| declaration.name)
        .collect();
    for update in updates {
        if update
            .metadata
            .keys()
            .any(|key| computed_columns::is_declaration_key(key))
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "metadata keys of a computed-column declaration cannot be edited \
                     (path '{}'); drop the column and declare it again",
                    update.path
                ),
            });
        }
        if update.replace
            && declared
                .iter()
                .any(|name| *name == computed_columns::root(&update.path))
        {
            return Err(Error::InvalidInput {
                message: format!(
                    "replacing all metadata of computed column '{}' would erase its \
                     declaration; drop the column and declare it again",
                    update.path
                ),
            });
        }
    }

    let mut builder = dataset.update_field_metadata();
    for update in updates {
        let entries = update.metadata.iter().map(|(k, v)| (k.clone(), v.clone()));
        builder = if update.replace {
            builder.replace(&update.path, entries)?
        } else {
            builder.update(&update.path, entries)?
        };
    }
    builder.await?;

    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(UpdateFieldMetadataResult { version })
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, StringArray, record_batch};
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance::dataset::ColumnAlteration;

    use super::FieldMetadataUpdate;
    use crate::connect;
    use crate::function::FunctionBinding;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::NewColumnTransform;
    use crate::table::computed_columns::{
        FUNCTION_ASSIGNMENT_OUTPUT_ORDINAL, FUNCTION_BINDINGS_META_KEY,
        ensure_supported_function_metadata, function_bindings, function_bindings_metadata,
        function_computed_column_metadata, is_declaration_key,
    };
    use crate::{Error, Table};
    use std::collections::HashMap;

    /// A table carrying the fixture binding: `title` and `body` are its
    /// inputs, `search_text` and `search_token_count` its outputs, `spare`
    /// nobody's. Stamped the way the server does it, since no local path
    /// declares a binding.
    async fn bound_table() -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("title", Utf8, ["a"]),
            ("body", Utf8, ["b"]),
            ("search_text", Utf8, ["a b"]),
            ("search_token_count", Int64, [2]),
            ("spare", Int32, [1])
        )
        .unwrap();
        let table = conn.create_table("bound", batch).execute().await.unwrap();
        stamp_bindings(&table, &[text_features_binding("")]).await;
        table
    }

    /// The fixture binding, its id and output names suffixed so a table can
    /// carry more than one.
    fn text_features_binding(suffix: &str) -> FunctionBinding {
        let raw = include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        );
        if suffix.is_empty() {
            return FunctionBinding::from_json(raw).unwrap();
        }
        FunctionBinding::from_json(
            &raw.replace("fb_01K3TEXT", &format!("fb_01K3TEXT{suffix}"))
                .replace("search_text", &format!("search_text{suffix}"))
                .replace("search_token_count", &format!("search_token_count{suffix}")),
        )
        .unwrap()
    }

    /// Stamp bindings the way the server does it, since no local path
    /// declares one: the envelope on the schema, a declaration marker on
    /// every output field.
    async fn stamp_bindings(table: &Table, bindings: &[FunctionBinding]) {
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        dataset
            .update_schema_metadata(vec![(
                FUNCTION_BINDINGS_META_KEY.to_string(),
                Some(function_bindings_metadata(bindings).unwrap()),
            )])
            .await
            .unwrap();
        let inputs = ["title".to_string(), "body".to_string()];
        let outputs = bindings
            .iter()
            .flat_map(|binding| {
                let assignment = binding.assignment().map(|assignment| {
                    (
                        assignment.output_name.clone(),
                        FUNCTION_ASSIGNMENT_OUTPUT_ORDINAL,
                    )
                });
                binding
                    .outputs()
                    .iter()
                    .map(|output| (output.output_name.clone(), output.output_ordinal))
                    .chain(assignment)
                    .map(|(name, ordinal)| {
                        (
                            dataset.schema().field(&name).unwrap().id as u32,
                            function_computed_column_metadata(
                                binding.binding_id(),
                                ordinal,
                                &inputs,
                            ),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        dataset.replace_field_metadata(outputs).await.unwrap();
        native.dataset.update(dataset);
        ensure_supported_function_metadata(&table.schema().await.unwrap()).unwrap();
    }

    fn metadata_update(path: &str) -> FieldMetadataUpdate {
        FieldMetadataUpdate {
            path: path.into(),
            metadata: HashMap::from([("unit".to_string(), Some("label".to_string()))]),
            replace: false,
        }
    }

    /// Columns no binding uses evolve as on any table, and the binding is
    /// still valid afterwards, which is what every later write checks.
    #[tokio::test]
    async fn test_schema_evolution_leaves_unbound_columns_free_on_a_bound_table() {
        let table = bound_table().await;
        table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "eager".into(),
                "1".into(),
            )]))
            .execute()
            .await
            .unwrap();
        table
            .add_columns()
            .computed("derived", "spare * 2")
            .execute()
            .await
            .unwrap();
        table
            .update_field_metadata(&[metadata_update("eager")])
            .await
            .unwrap();
        table
            .alter_columns(&[ColumnAlteration::new("eager".into()).rename("moved".into())])
            .await
            .unwrap();
        table.drop_columns(&["moved"]).await.unwrap();

        let schema = table.schema().await.unwrap();
        ensure_supported_function_metadata(&schema).unwrap();
        assert_eq!(function_bindings(&schema).unwrap().len(), 1);
        assert!(schema.field_with_name("derived").is_ok());
        assert!(schema.field_with_name("moved").is_err());
    }

    fn bound(err: Error) {
        assert!(
            matches!(&err, Error::InvalidInput { message }
                if message.contains("a Function binding reads or writes it")),
            "{err:?}"
        );
    }

    fn incomplete_group(err: Error) {
        assert!(
            matches!(&err, Error::InvalidInput { message }
                if message.contains("must drop every output of its binding")),
            "{err:?}"
        );
    }

    async fn bindings_of(table: &Table) -> Vec<FunctionBinding> {
        function_bindings(table.schema().await.unwrap().as_ref()).unwrap()
    }

    /// Every schema-evolution door refuses a column a binding reads or
    /// writes, including a rename onto one.
    #[tokio::test]
    async fn test_schema_evolution_refuses_the_columns_a_function_binding_uses() {
        let table = bound_table().await;
        let version = table.version().await.unwrap();
        for column in ["title", "body", "search_text", "search_token_count"] {
            // A lone drop of an output is refused too, but for its siblings
            // rather than for the binding; see the retirement tests below.
            let err = table.drop_columns(&[column]).await.unwrap_err();
            if column.starts_with("search_") {
                incomplete_group(err);
            } else {
                bound(err);
            }
            bound(
                table
                    .alter_columns(&[ColumnAlteration::new(column.into()).rename("moved".into())])
                    .await
                    .unwrap_err(),
            );
            bound(
                table
                    .alter_columns(&[ColumnAlteration::new(column.into()).set_nullable(false)])
                    .await
                    .unwrap_err(),
            );
            bound(
                table
                    .update_field_metadata(&[metadata_update(column)])
                    .await
                    .unwrap_err(),
            );
            bound(
                table
                    .add_columns()
                    .transform(NewColumnTransform::SqlExpressions(vec![(
                        column.into(),
                        "1".into(),
                    )]))
                    .execute()
                    .await
                    .unwrap_err(),
            );
            bound(
                table
                    .add_columns()
                    .computed(column, "1")
                    .execute()
                    .await
                    .unwrap_err(),
            );
        }
        bound(
            table
                .alter_columns(&[ColumnAlteration::new("spare".into()).rename("title".into())])
                .await
                .unwrap_err(),
        );
        assert_eq!(table.version().await.unwrap(), version);
    }

    /// Lance resolves a quoted spelling to the same field as the bare one,
    /// so the guard compares identities, not text.
    #[tokio::test]
    async fn quoted_function_output_path_is_still_refused() {
        let table = bound_table().await;
        let version = table.version().await.unwrap();
        for path in ["`title`", "`search_text`", "`title`.nested"] {
            let err = table.drop_columns(&[path]).await.unwrap_err();
            if path == "`search_text`" {
                incomplete_group(err);
            } else {
                bound(err);
            }
            bound(
                table
                    .alter_columns(&[ColumnAlteration::new(path.into()).set_nullable(false)])
                    .await
                    .unwrap_err(),
            );
            bound(
                table
                    .update_field_metadata(&[metadata_update(path)])
                    .await
                    .unwrap_err(),
            );
        }
        bound(
            table
                .alter_columns(&[ColumnAlteration::new("spare".into()).rename("`title`".into())])
                .await
                .unwrap_err(),
        );
        assert_eq!(table.version().await.unwrap(), version);
    }

    // Add Columns Tests

    #[tokio::test]
    async fn test_add_columns_with_sql_expression() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3, 4, 5])).unwrap();

        let table = conn
            .create_table("test_add_columns", batch)
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Add a computed column
        let result = table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "doubled".into(),
                "id * 2".into(),
            )]))
            .execute()
            .await
            .unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify the new column exists with correct values
        let batches = table
            .query()
            .select(Select::columns(&["id", "doubled"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let ids: Vec<i32> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        let doubled: Vec<i32> = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();

        for (id, d) in ids.iter().zip(doubled.iter()) {
            assert_eq!(*d, id * 2);
        }
    }

    #[tokio::test]
    async fn test_add_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("x", Int32, [10, 20, 30])).unwrap();

        let table = conn
            .create_table("test_add_multi_columns", batch)
            .execute()
            .await
            .unwrap();

        // Add multiple columns at once
        table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![
                ("y".into(), "x + 1".into()),
                ("z".into(), "x * x".into()),
            ]))
            .execute()
            .await
            .unwrap();

        // Verify schema has all columns
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("x").is_ok());
        assert!(schema.field_with_name("y").is_ok());
        assert!(schema.field_with_name("z").is_ok());
    }

    #[tokio::test]
    async fn test_add_column_with_constant_expression() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_add_const_column", batch)
            .execute()
            .await
            .unwrap();

        // Add a column with a constant value
        table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "constant".into(),
                "42".into(),
            )]))
            .execute()
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("constant").is_ok());

        // Verify all values are 42
        let batches = table
            .query()
            .select(Select::columns(&["constant"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        let values = batch["constant"]
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values();
        assert!(values.iter().all(|&v| v == 42));
    }

    // Alter Columns Tests

    #[tokio::test]
    async fn test_alter_column_rename() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("old_name", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_alter_rename", batch)
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Rename the column
        let result = table
            .alter_columns(&[ColumnAlteration::new("old_name".into()).rename("new_name".into())])
            .await
            .unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify rename
        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("old_name").is_err());
        assert!(schema.field_with_name("new_name").is_ok());
    }

    #[tokio::test]
    async fn test_alter_column_set_nullable() {
        use arrow_array::RecordBatch;
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let conn = connect("memory://").execute().await.unwrap();

        // Create a schema with a non-nullable field
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let table = conn
            .create_table("test_alter_nullable", batch)
            .execute()
            .await
            .unwrap();

        // Initially non-nullable
        let schema = table.schema().await.unwrap();
        assert!(!schema.field_with_name("value").unwrap().is_nullable());

        // Make it nullable
        table
            .alter_columns(&[ColumnAlteration::new("value".into()).set_nullable(true)])
            .await
            .unwrap();

        // Verify it's now nullable
        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("value").unwrap().is_nullable());
    }

    #[tokio::test]
    async fn test_alter_column_cast_type() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("num", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_cast_type", batch)
            .execute()
            .await
            .unwrap();

        // Cast Int32 to Int64 (a supported cast)
        table
            .alter_columns(&[ColumnAlteration::new("num".into()).cast_to(DataType::Int64)])
            .await
            .unwrap();

        // Verify type changed
        let schema = table.schema().await.unwrap();
        assert_eq!(
            schema.field_with_name("num").unwrap().data_type(),
            &DataType::Int64
        );

        // Query the data and verify the returned type is correct
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &batches[0];
        let values = batch["num"]
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values();
        assert_eq!(values.as_ref(), &[1i64, 2, 3]);
    }

    #[tokio::test]
    async fn test_alter_column_invalid_cast_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("num", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_invalid_cast", batch)
            .execute()
            .await
            .unwrap();

        // Casting Int32 to Float64 is not supported
        let result = table
            .alter_columns(&[ColumnAlteration::new("num".into()).cast_to(DataType::Float64)])
            .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cast"),
            "Expected error message to contain 'cast', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_alter_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [4, 5, 6])).unwrap();

        let table = conn
            .create_table("test_alter_multi", batch)
            .execute()
            .await
            .unwrap();

        // Alter multiple columns at once
        table
            .alter_columns(&[
                ColumnAlteration::new("a".into()).rename("alpha".into()),
                ColumnAlteration::new("b".into()).set_nullable(true),
            ])
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("alpha").is_ok());
        assert!(schema.field_with_name("a").is_err());
        assert!(schema.field_with_name("b").unwrap().is_nullable());
    }

    // Drop Columns Tests

    #[tokio::test]
    async fn test_drop_single_column() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch =
            record_batch!(("keep", Int32, [1, 2, 3]), ("remove", Int32, [4, 5, 6])).unwrap();

        let table = conn
            .create_table("test_drop_single", batch)
            .execute()
            .await
            .unwrap();

        let initial_version = table.version().await.unwrap();

        // Drop a column
        let result = table.drop_columns(&["remove"]).await.unwrap();

        // Version should increment
        assert!(result.version > initial_version);

        // Verify column was dropped
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert!(schema.field_with_name("keep").is_ok());
        assert!(schema.field_with_name("remove").is_err());
    }

    #[tokio::test]
    async fn test_drop_multiple_columns() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(
            ("a", Int32, [1, 2]),
            ("b", Int32, [3, 4]),
            ("c", Int32, [5, 6]),
            ("d", Int32, [7, 8])
        )
        .unwrap();

        let table = conn
            .create_table("test_drop_multi", batch)
            .execute()
            .await
            .unwrap();

        // Drop multiple columns
        table.drop_columns(&["b", "d"]).await.unwrap();

        // Verify only a and c remain
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name("a").is_ok());
        assert!(schema.field_with_name("c").is_ok());
        assert!(schema.field_with_name("b").is_err());
        assert!(schema.field_with_name("d").is_err());
    }

    #[tokio::test]
    async fn test_drop_column_preserves_data() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("name", Utf8, ["a", "b", "c"]),
            ("extra", Int32, [10, 20, 30])
        )
        .unwrap();

        let table = conn
            .create_table("test_drop_preserves", batch)
            .execute()
            .await
            .unwrap();

        // Drop the extra column
        table.drop_columns(&["extra"]).await.unwrap();

        // Verify remaining data is intact
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);

        let ids: Vec<i32> = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);

        let names: Vec<&str> = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    // Error Case Tests

    #[tokio::test]
    async fn test_drop_nonexistent_column_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("existing", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_drop_nonexistent", batch)
            .execute()
            .await
            .unwrap();

        // Try to drop a column that doesn't exist
        let result = table.drop_columns(&["nonexistent"]).await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "Expected error message to contain column name 'nonexistent', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_alter_nonexistent_column_fails() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("existing", Int32, [1, 2, 3])).unwrap();

        let table = conn
            .create_table("test_alter_nonexistent", batch)
            .execute()
            .await
            .unwrap();

        // Try to alter a column that doesn't exist
        let result = table
            .alter_columns(&[ColumnAlteration::new("nonexistent".into()).rename("new".into())])
            .await;
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("nonexistent"),
            "Expected error message to contain column name 'nonexistent', got: {}",
            err
        );
    }

    // Version Tracking Tests

    #[tokio::test]
    async fn test_schema_operations_increment_version() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("a", Int32, [1, 2, 3]), ("b", Int32, [4, 5, 6])).unwrap();
        let table = conn
            .create_table("test_version_increment", batch)
            .execute()
            .await
            .unwrap();

        let v1 = table.version().await.unwrap();

        // Add column increments version
        let add_result = table
            .add_columns()
            .transform(NewColumnTransform::SqlExpressions(vec![(
                "c".into(),
                "a + b".into(),
            )]))
            .execute()
            .await
            .unwrap();
        assert!(add_result.version > v1);
        let v2 = table.version().await.unwrap();
        assert_eq!(add_result.version, v2);

        // Alter column increments version
        let alter_result = table
            .alter_columns(&[ColumnAlteration::new("c".into()).rename("sum".into())])
            .await
            .unwrap();
        assert!(alter_result.version > v2);
        let v3 = table.version().await.unwrap();
        assert_eq!(alter_result.version, v3);

        // Drop column increments version
        let drop_result = table.drop_columns(&["b"]).await.unwrap();
        assert!(drop_result.version > v3);
        let v4 = table.version().await.unwrap();
        assert_eq!(drop_result.version, v4);
    }

    // Retiring a Function binding by dropping its outputs (ENT-2669).

    /// The whole output set goes, and the binding with it: no envelope, no
    /// declaration markers, and the inputs it held are ordinary again.
    #[tokio::test]
    async fn dropping_every_output_retires_the_binding() {
        let table = bound_table().await;
        table
            .drop_columns(&["search_text", "search_token_count"])
            .await
            .unwrap();

        let schema = table.schema().await.unwrap();
        assert!(schema.field_with_name("search_text").is_err());
        assert!(schema.field_with_name("search_token_count").is_err());
        assert!(!schema.metadata().contains_key(FUNCTION_BINDINGS_META_KEY));
        assert!(bindings_of(&table).await.is_empty());

        // What the binding used to protect is now ordinary schema.
        table.drop_columns(&["title"]).await.unwrap();
        table
            .update_field_metadata(&[metadata_update("body")])
            .await
            .unwrap();
        ensure_supported_function_metadata(&table.schema().await.unwrap()).unwrap();
    }

    /// One output of a multi-output binding cannot go alone: its siblings are
    /// written by the same refresh in the same commit.
    #[tokio::test]
    async fn dropping_part_of_an_output_group_names_the_missing_siblings() {
        let table = bound_table().await;
        let version = table.version().await.unwrap();
        for (named, missing) in [
            ("search_text", "search_token_count"),
            ("search_token_count", "search_text"),
        ] {
            let err = table.drop_columns(&[named]).await.unwrap_err();
            let Error::InvalidInput { message } = &err else {
                panic!("{err:?}");
            };
            assert!(
                message.contains("must drop every output of its binding")
                    && message.contains(missing),
                "{message}"
            );
        }
        assert_eq!(table.version().await.unwrap(), version);
        assert_eq!(bindings_of(&table).await.len(), 1);
    }

    /// An input is protected by the binding, not by its own name, so it drops
    /// in the request that retires the binding reading it.
    #[tokio::test]
    async fn an_input_drops_with_the_binding_that_reads_it() {
        let table = bound_table().await;
        bound(table.drop_columns(&["title"]).await.unwrap_err());

        table
            .drop_columns(&["title", "body", "search_text", "search_token_count"])
            .await
            .unwrap();
        let schema = table.schema().await.unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert!(schema.field_with_name("spare").is_ok());
        assert!(bindings_of(&table).await.is_empty());
    }

    /// Lance resolves a quoted spelling to the same field, so it retires the
    /// same binding rather than slipping past the group rule.
    #[tokio::test]
    async fn a_quoted_output_spelling_retires_the_same_binding() {
        let table = bound_table().await;
        incomplete_group(table.drop_columns(&["`search_text`"]).await.unwrap_err());
        table
            .drop_columns(&["`search_text`", "`search_token_count`"])
            .await
            .unwrap();
        assert!(bindings_of(&table).await.is_empty());
    }

    /// The unbind lands before the drop, and on its own: at that version the
    /// outputs are still there as plain columns, with no binding and no
    /// declaration metadata left pointing at one. A crash in between leaves
    /// that state, which every later write must accept.
    #[tokio::test]
    async fn the_unbind_commit_leaves_a_valid_table_before_the_drop() {
        let table = bound_table().await;
        let before = table.version().await.unwrap();
        table
            .drop_columns(&["search_text", "search_token_count"])
            .await
            .unwrap();
        assert_eq!(table.version().await.unwrap(), before + 2);

        table.checkout(before + 1).await.unwrap();
        let schema = table.schema().await.unwrap();
        assert!(!schema.metadata().contains_key(FUNCTION_BINDINGS_META_KEY));
        for column in ["search_text", "search_token_count"] {
            let field = schema.field_with_name(column).unwrap();
            assert!(
                !field.metadata().keys().any(|key| is_declaration_key(key)),
                "{column}: {:?}",
                field.metadata()
            );
        }
        ensure_supported_function_metadata(&schema).unwrap();
        assert!(function_bindings(&schema).unwrap().is_empty());
    }

    /// A table carrying two bindings retires only the one whose outputs the
    /// drop names; the other keeps its envelope entry and its protection.
    #[tokio::test]
    async fn retiring_one_binding_leaves_the_others_standing() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("title", Utf8, ["a"]),
            ("body", Utf8, ["b"]),
            ("search_text", Utf8, ["a b"]),
            ("search_token_count", Int64, [2]),
            ("search_text_2", Utf8, ["a b"]),
            ("search_token_count_2", Int64, [2])
        )
        .unwrap();
        let table = conn.create_table("two", batch).execute().await.unwrap();
        stamp_bindings(
            &table,
            &[text_features_binding(""), text_features_binding("_2")],
        )
        .await;

        table
            .drop_columns(&["search_text", "search_token_count"])
            .await
            .unwrap();
        let remaining = bindings_of(&table).await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].binding_id(), "fb_01K3TEXT_2");
        // The survivor still protects the inputs it shared with the retired one.
        bound(table.drop_columns(&["title"]).await.unwrap_err());
        // And its own outputs are still a group.
        incomplete_group(table.drop_columns(&["search_text_2"]).await.unwrap_err());
    }

    /// A multi-output binding also owns a hidden `__function_assignment_*`
    /// column, which the caller never names. Retiring the binding has to take
    /// it: leaving it behind orphans a column nothing can fill or explain.
    #[tokio::test]
    async fn retiring_a_binding_drops_its_assignment_column() {
        const ASSIGNMENT: &str = "__function_assignment_fb_01K3TEXT";

        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("title", Utf8, ["a"]),
            ("body", Utf8, ["b"]),
            ("search_text", Utf8, ["a b"]),
            ("search_token_count", Int64, [2]),
            (ASSIGNMENT, Boolean, [Some(true)]),
            ("spare", Int32, [1])
        )
        .unwrap();
        let table = conn
            .create_table("assigned", batch)
            .execute()
            .await
            .unwrap();

        let raw = include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        )
        .replace(
            r#""future_binding""#,
            &format!(
                r#""assignment": {{"output_name": "{ASSIGNMENT}", "output_field_id": -1}}, "future_binding""#
            ),
        )
        // The assignment column is a physical sibling, so it belongs to the
        // binding's output schema too -- the server writes it that way.
        .replace(
            r#"{"name": "search_token_count", "nullable": true, "type": {"type": "int64"}}"#,
            &format!(
                r#"{{"name": "search_token_count", "nullable": true, "type": {{"type": "int64"}}}}, {{"name": "{ASSIGNMENT}", "nullable": true, "type": {{"type": "bool"}}}}"#
            ),
        );
        let binding = FunctionBinding::from_json(&raw).unwrap();
        assert!(binding.assignment().is_some(), "fixture must carry one");
        stamp_bindings(&table, std::slice::from_ref(&binding)).await;

        // The assignment column is the binding's, so it is protected too...
        bound(table.drop_columns(&[ASSIGNMENT]).await.unwrap_err());

        // ...and goes with the outputs without the caller naming it.
        table
            .drop_columns(&["search_text", "search_token_count"])
            .await
            .unwrap();
        let schema = table.schema().await.unwrap();
        assert!(
            schema.field_with_name(ASSIGNMENT).is_err(),
            "the assignment column outlived its binding: {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        );
        assert!(bindings_of(&table).await.is_empty());
        ensure_supported_function_metadata(&schema).unwrap();
    }

    #[tokio::test]
    async fn test_update_field_metadata() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("category", Utf8, ["A", "B", "C"])
        )
        .unwrap();
        let table = conn
            .create_table("test_update_field_metadata", batch)
            .execute()
            .await
            .unwrap();

        // Set metadata on a field.
        table
            .update_field_metadata(&[FieldMetadataUpdate::new("category")
                .set("unit", "label")
                .set("pii", "false")])
            .await
            .unwrap();
        let schema = table.schema().await.unwrap();
        let field = schema.field_with_name("category").unwrap();
        assert_eq!(
            field.metadata().get("unit").map(String::as_str),
            Some("label")
        );

        // Merge: add a key, delete one, keep the rest.
        table
            .update_field_metadata(&[FieldMetadataUpdate::new("category")
                .set("source", "import")
                .remove("pii")])
            .await
            .unwrap();
        let schema = table.schema().await.unwrap();
        let md = schema.field_with_name("category").unwrap().metadata();
        assert_eq!(md.get("unit").map(String::as_str), Some("label")); // preserved
        assert_eq!(md.get("source").map(String::as_str), Some("import")); // added
        assert!(!md.contains_key("pii")); // deleted
    }
}
