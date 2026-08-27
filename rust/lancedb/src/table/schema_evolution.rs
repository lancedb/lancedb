// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Schema evolution operations for LanceDB tables.
//!
//! This module provides functionality to modify the schema of existing tables:
//! - [`add_columns`](execute_add_columns): Add new columns using SQL expressions
//! - [`alter_columns`](execute_alter_columns): Rename columns, change types, or modify nullability
//! - [`drop_columns`](execute_drop_columns): Remove columns from the table

use arrow_schema::Schema as ArrowSchema;
use lance::dataset::{ColumnAlteration, NewColumnTransform};
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
    computed_columns::ensure_no_function_bindings_for_mutation(
        table.schema().await?.as_ref(),
        "schema evolution",
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
    computed_columns::ensure_no_function_bindings_for_mutation(
        table.schema().await?.as_ref(),
        "schema evolution",
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
    // Nullability is not part of what an expression resolves against, so only
    // a rename or a retype can invalidate a binding.
    let schema = std::sync::Arc::new(ArrowSchema::from(dataset.schema()));
    computed_columns::ensure_no_function_bindings_for_mutation(
        schema.as_ref(),
        "schema evolution",
    )?;
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
    computed_columns::ensure_no_function_bindings_for_mutation(
        &ArrowSchema::from(dataset.schema()),
        "schema evolution",
    )?;
    computed_columns::ensure_not_an_input(
        &std::sync::Arc::new(ArrowSchema::from(dataset.schema())),
        columns,
    )?;
    dataset.drop_columns(columns).await?;
    let version = dataset.version().version;
    table.dataset.update(dataset);
    Ok(DropColumnsResult { version })
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
    computed_columns::ensure_no_function_bindings_for_mutation(&schema, "schema evolution")?;
    let declared: Vec<String> = computed_columns::write_protected_columns(&schema);
    for update in updates {
        // Set and delete alike: a removal arrives as a `None`-valued key.
        if update
            .metadata
            .keys()
            .any(|key| computed_columns::is_write_protection_key(key))
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
                .any(|name| name == computed_columns::root(&update.path))
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
    use std::collections::HashMap;

    use arrow_array::{Int32Array, RecordBatchIterator, StringArray, record_batch};
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use lance::dataset::ColumnAlteration;

    use super::FieldMetadataUpdate;
    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::computed_columns::test_support::tag_field;
    use crate::table::{NewColumnTransform, Table};

    /// A table whose `y` column carries the legacy `virtual_column` marker.
    async fn legacy_table(name: &str, extra: &[(&str, &str)]) -> Table {
        let mut metadata = vec![("virtual_column", "true")];
        metadata.extend_from_slice(extra);
        let batch = tag_field(
            record_batch!(("x", Int32, [1]), ("y", Int32, [None])).unwrap(),
            1,
            &metadata,
        );
        connect("memory://")
            .execute()
            .await
            .unwrap()
            .create_table(name, batch)
            .execute()
            .await
            .unwrap()
    }

    /// The write guard still refuses a direct value for `y`.
    async fn assert_direct_write_refused(table: &Table) {
        let values = record_batch!(("x", Int32, [2]), ("y", Int32, [Some(999)])).unwrap();
        let err = table
            .add(values)
            .execute()
            .await
            .expect_err("direct write to the legacy column must be refused");
        assert!(err.to_string().contains("computed"), "{err}");
    }

    /// The lance field id of `name`.
    async fn field_id(table: &Table, name: &str) -> u32 {
        table
            .as_native()
            .unwrap()
            .manifest()
            .await
            .unwrap()
            .schema
            .fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| field.id as u32)
            .unwrap()
    }

    /// A legacy-tagged column's keys are immutable through the field-metadata
    /// API, like a modern declaration's, and the write guard holds after each.
    #[tokio::test]
    async fn legacy_marker_survives_field_metadata_mutation() {
        let table = legacy_table("legacy_meta", &[]).await;

        // Removing the marker (a None-valued key) is refused.
        let mut removal = FieldMetadataUpdate::new("y");
        removal.metadata.insert("virtual_column".to_string(), None);
        let err = table
            .update_field_metadata(&[removal])
            .await
            .expect_err("removing the legacy marker must be refused");
        assert!(err.to_string().contains("declaration"), "{err}");

        // Editing a legacy declaration key is refused.
        let edit = FieldMetadataUpdate::new("y").set("virtual_column.expression", "x * 3");
        let err = table
            .update_field_metadata(&[edit])
            .await
            .expect_err("editing a legacy declaration key must be refused");
        assert!(err.to_string().contains("declaration"), "{err}");

        // Replacing the field's whole metadata map is refused.
        let mut replace = FieldMetadataUpdate::new("y").set("note", "hi");
        replace.replace = true;
        let err = table
            .update_field_metadata(&[replace])
            .await
            .expect_err("replace on a write-protected field must be refused");
        assert!(err.to_string().contains("erase"), "{err}");

        assert_direct_write_refused(&table).await;
    }

    /// Casting a legacy-tagged column is refused: the cast would erase its
    /// marker.
    #[tokio::test]
    async fn legacy_marker_survives_alter_columns_cast() {
        let table = legacy_table("legacy_cast", &[]).await;

        let err = table
            .alter_columns(&[ColumnAlteration::new("y".into()).cast_to(DataType::Int64)])
            .await
            .expect_err("casting the legacy column must be refused");
        assert!(err.to_string().contains("computed"), "{err}");

        assert_direct_write_refused(&table).await;
    }

    /// The deprecated field-id replacement is bound by the same invariant:
    /// stripping a protection marker through it is refused.
    #[tokio::test]
    async fn legacy_marker_survives_deprecated_replace_field_metadata() {
        let table = legacy_table("legacy_replace", &[]).await;
        let y_id = field_id(&table, "y").await;

        #[allow(deprecated)]
        let err = table
            .as_native()
            .unwrap()
            .replace_field_metadata(vec![(y_id, HashMap::new())])
            .await
            .expect_err("replacing the legacy field's metadata must be refused");
        assert!(err.to_string().contains("declaration"), "{err}");

        assert_direct_write_refused(&table).await;
    }

    /// The column join is a write path: right-side values are refused too.
    #[tokio::test]
    async fn legacy_column_rejects_merge_join_values() {
        let table = legacy_table("legacy_merge_join", &[]).await;
        let right = record_batch!(("x", Int32, [1]), ("y", Int32, [Some(999)])).unwrap();
        let err = table
            .as_native()
            .unwrap()
            .clone()
            .merge(
                RecordBatchIterator::new(vec![Ok(right.clone())], right.schema()),
                "x",
                "x",
            )
            .await
            .expect_err("merging values into the legacy column must be refused");
        assert!(err.to_string().contains("computed"), "{err}");
    }

    /// A fabricated marker through the deprecated field-id replacement would
    /// wedge the table, so it is refused.
    #[tokio::test]
    async fn field_metadata_replacement_rejects_fabricated_markers() {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int32, [1, 2]), ("v", Int32, [10, 20])).unwrap();
        let table = conn
            .create_table("fabricate_marker", batch)
            .execute()
            .await
            .unwrap();
        let v_id = field_id(&table, "v").await;

        for key in [
            "virtual_column",
            "computed_column",
            "virtual_column.expression",
        ] {
            #[allow(deprecated)]
            let err = table
                .as_native()
                .unwrap()
                .replace_field_metadata(vec![(
                    v_id,
                    HashMap::from([(key.to_string(), "true".to_string())]),
                )])
                .await
                .expect_err("fabricating a declaration marker must be refused");
            assert!(err.to_string().contains("declaration"), "{key}: {err}");
        }
    }

    /// A column join cannot introduce declaration-tagged fields either.
    #[tokio::test]
    async fn merge_join_rejects_fabricated_markers() {
        let conn = connect("memory://").execute().await.unwrap();
        let table = conn
            .create_table(
                "fabricate_merge",
                record_batch!(("x", Int32, [1, 2])).unwrap(),
            )
            .execute()
            .await
            .unwrap();
        let right = tag_field(
            record_batch!(("x", Int32, [1]), ("planted", Int32, [Some(7)])).unwrap(),
            1,
            &[("virtual_column", "true")],
        );
        let err = table
            .as_native()
            .unwrap()
            .clone()
            .merge(
                RecordBatchIterator::new(vec![Ok(right.clone())], right.schema()),
                "x",
                "x",
            )
            .await
            .expect_err("merging a declaration-tagged column must be refused");
        assert!(err.to_string().contains("declaration"), "{err}");
    }

    /// A column a legacy declaration records as an input is protected from
    /// rename, retype, and drop, matching the modern input policy.
    #[tokio::test]
    async fn legacy_recorded_inputs_are_protected() {
        let table = legacy_table("legacy_inputs", &[("virtual_column.inputs", r#"["x"]"#)]).await;

        let err = table
            .alter_columns(&[ColumnAlteration::new("x".into()).rename("x2".into())])
            .await
            .expect_err("renaming a recorded legacy input must be refused");
        assert!(err.to_string().contains("is read by"), "{err}");

        let err = table
            .drop_columns(&["x"])
            .await
            .expect_err("dropping a recorded legacy input must be refused");
        assert!(err.to_string().contains("is read by"), "{err}");
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
