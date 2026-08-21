// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! One-fragment publication for registered Function columns.
//!
//! This module publishes one complete, physical-row-aligned sibling group with
//! one `DataReplacement` transaction. Planning, execution, retry, and
//! multi-fragment aggregation remain outside this module.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::{Stream, TryStreamExt, stream};
use lance::Dataset;
use lance::dataset::CommitBuilder;
use lance::dataset::fragment::FileFragment;
use lance::dataset::transaction::{DataReplacementGroup, Operation, TransactionBuilder};
use lance_core::ROW_ID;
use lance_core::datatypes::Schema as LanceSchema;
use lance_namespace::models::JsonArrowSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::computed_columns::{ensure_supported_function_metadata, function_bindings};
use super::refresh::ensure_no_lsm_write_spec;
use super::{NativeTable, Table};
use crate::function::FunctionBinding;
use crate::{Error, Result};

const TX_KIND: &str = "lancedb.fragment-publication.v1";
const TX_KIND_KEY: &str = "lancedb.transaction.kind";
const TX_JOB_KEY: &str = "lancedb.fragment-publication.job-id";
const TX_ATTEMPT_KEY: &str = "lancedb.fragment-publication.attempt-id";
const TX_BASIS_KEY: &str = "lancedb.fragment-publication.basis-digest";
const TX_FRAGMENT_KEY: &str = "lancedb.fragment-publication.fragment-id";
const TX_OUTPUTS_KEY: &str = "lancedb.fragment-publication.output-field-paths";

/// One V1 parameter-to-field locator frozen into a publication basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentInputBinding {
    pub parameter: String,
    pub field_path: String,
    pub arrow_type: String,
    pub nullable: bool,
}

/// One ordered sibling output locator frozen into a publication basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FragmentOutputBinding {
    pub result_field: String,
    pub field_path: String,
    pub output_ordinal: u32,
    pub arrow_type: String,
    /// Logical Function nullability. V1 requires `false`; the physical table
    /// field remains nullable while NULL represents unassigned.
    pub nullable: bool,
}

/// Immutable source and binding witness for one fragment publication.
///
/// Field paths, exact Arrow schemas, and nullability are the V1 locators. No
/// stable field ID is required or persisted by this contract.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FragmentPublicationBasis {
    source_version: u64,
    fragment_id: u64,
    physical_rows: u64,
    deletion_state_digest: String,
    fragment_digest: String,
    binding_id: String,
    binding_revision: u64,
    input_bindings: Vec<FragmentInputBinding>,
    output_bindings: Vec<FragmentOutputBinding>,
    input_schema: Value,
    output_schema: Value,
    basis_digest: String,
}

impl FragmentPublicationBasis {
    pub fn source_version(&self) -> u64 {
        self.source_version
    }

    pub fn fragment_id(&self) -> u64 {
        self.fragment_id
    }

    pub fn physical_rows(&self) -> u64 {
        self.physical_rows
    }

    pub fn deletion_state_digest(&self) -> &str {
        &self.deletion_state_digest
    }

    pub fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub fn binding_revision(&self) -> u64 {
        self.binding_revision
    }

    pub fn input_bindings(&self) -> &[FragmentInputBinding] {
        &self.input_bindings
    }

    pub fn output_bindings(&self) -> &[FragmentOutputBinding] {
        &self.output_bindings
    }

    pub fn input_schema(&self) -> &Value {
        &self.input_schema
    }

    pub fn output_schema(&self) -> &Value {
        &self.output_schema
    }

    pub fn basis_digest(&self) -> &str {
        &self.basis_digest
    }

    pub fn output_field_paths(&self) -> Vec<String> {
        self.output_bindings
            .iter()
            .map(|output| output.field_path.clone())
            .collect()
    }
}

/// Required identifiers and accounting for one publication attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FragmentPublicationOptions {
    pub job_id: String,
    pub attempt_id: String,
    pub transaction_uuid: String,
    pub basis: FragmentPublicationBasis,
    pub rows_assigned: u64,
}

impl FragmentPublicationOptions {
    pub fn new(
        job_id: impl Into<String>,
        attempt_id: impl Into<String>,
        basis: FragmentPublicationBasis,
        rows_assigned: u64,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            attempt_id: attempt_id.into(),
            transaction_uuid: Uuid::new_v4().hyphenated().to_string(),
            basis,
            rows_assigned,
        }
    }

    /// Override the generated UUID when resuming or reading back an attempt.
    pub fn with_transaction_uuid(mut self, transaction_uuid: impl Into<String>) -> Self {
        self.transaction_uuid = transaction_uuid.into();
        self
    }
}

/// Durable classification of an attempted transaction UUID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommitOutcome {
    Committed,
    Foreign {
        transaction_uuid: String,
        published_version: u64,
    },
    Absent,
    Unknown,
}

/// Frozen V1 receipt for one fragment publication attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub job_id: String,
    pub attempt_id: String,
    pub fragment_id: u64,
    pub basis_digest: String,
    pub transaction_uuid: String,
    pub output_field_paths: Vec<String>,
    /// Exact version for a committed or foreign publication. `None` is used
    /// only when the requested UUID is proven absent or its status is unknown.
    pub published_version: Option<u64>,
    pub rows_assigned: u64,
    pub commit_outcome: CommitOutcome,
}

/// One-fragment Function-column publisher.
#[derive(Debug, Clone)]
pub struct FragmentPublisher {
    table: Table,
}

impl FragmentPublisher {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Capture the exact source, fragment, deletion, and binding witness that
    /// worker execution must carry back to publication.
    pub async fn capture_basis(
        &self,
        binding_id: impl AsRef<str>,
        fragment_id: u64,
    ) -> Result<FragmentPublicationBasis> {
        let native = native_table(&self.table)?;
        native.dataset.ensure_mutable()?;
        ensure_no_lsm_write_spec(native).await?;
        let dataset = native.dataset.get().await?;
        capture_basis_from_dataset(&dataset, binding_id.as_ref(), fragment_id).await
    }

    /// Validate, stage, and atomically publish a complete sibling group.
    ///
    /// Validation and staging failures return `Err` before a transaction is
    /// submitted. Once submission begins, the returned receipt classifies the
    /// UUID as committed, foreign/conflicting, absent, or genuinely unknown.
    pub async fn publish(
        &self,
        options: FragmentPublicationOptions,
        output_batches: Vec<RecordBatch>,
    ) -> Result<CommitReceipt> {
        validate_identifiers(&options)?;
        let native = native_table(&self.table)?;
        native.dataset.ensure_mutable()?;
        ensure_no_lsm_write_spec(native).await?;

        let mut dataset = latest_dataset(native).await?;
        validate_basis(&dataset, &options.basis).await?;
        let output_schema = expected_output_schema(&options.basis)?;
        let output = validate_complete_output(
            &dataset,
            &options.basis,
            &output_schema,
            output_batches,
            options.rows_assigned,
        )
        .await?;

        let fragment = fragment(&dataset, options.basis.fragment_id)?;
        let lance_schema = output_lance_schema(&dataset, &options.basis)?;
        let replacement = stage_fragment_columns(
            &fragment,
            stream::iter(output.iter().cloned().map(Ok)),
            &lance_schema,
        )
        .await?;

        // Staging may take long enough for the table to move. Re-read the head
        // and repeat every source/binding/assignment witness immediately before
        // constructing the transaction.
        dataset = latest_dataset(native).await?;
        validate_basis(&dataset, &options.basis).await?;
        validate_unassigned_rows(&dataset, &options.basis, options.rows_assigned).await?;

        let properties = transaction_properties(&options)?;
        let transaction = TransactionBuilder::new(
            dataset.version().version,
            Operation::DataReplacement {
                replacements: vec![replacement],
            },
        )
        .uuid(options.transaction_uuid.clone())
        .transaction_properties(Some(Arc::new(properties)))
        .build();

        let committed = CommitBuilder::new(Arc::new(dataset.clone()))
            // Overlapping DataReplacement transactions conflict, and zero
            // retries keeps that conflict from becoming a blind latest-head
            // retry. Lance still performs one pre-commit conflict-resolution
            // pass; it has no strict expected-head/fragment-witness mode yet,
            // so a deletion landing after the validation above can be treated
            // as compatible inside that narrow race.
            .with_max_retries(0)
            .execute(transaction)
            .await;

        match committed {
            Ok(committed) => {
                let published_version = committed.version().version;
                native.dataset.update(committed);
                Ok(receipt(
                    &options,
                    Some(published_version),
                    CommitOutcome::Committed,
                ))
            }
            Err(error) => {
                let (mut outcome, mut published_version) =
                    read_outcome(native, &options.basis, &options.transaction_uuid).await;
                if error.is_commit_status_unknown() && matches!(outcome, CommitOutcome::Absent) {
                    outcome = CommitOutcome::Unknown;
                    published_version = None;
                }
                Ok(receipt(&options, published_version, outcome))
            }
        }
    }

    /// Read back a transaction UUID without submitting or retrying a commit.
    pub async fn read_commit_outcome(
        &self,
        basis: &FragmentPublicationBasis,
        transaction_uuid: &str,
    ) -> CommitOutcome {
        let Ok(native) = native_table(&self.table) else {
            return CommitOutcome::Unknown;
        };
        read_outcome(native, basis, transaction_uuid).await.0
    }
}

fn native_table(table: &Table) -> Result<&NativeTable> {
    table.as_native().ok_or_else(|| Error::NotSupported {
        message: "fragment publication requires a native table handle".to_string(),
    })
}

fn validate_identifiers(options: &FragmentPublicationOptions) -> Result<()> {
    if options.job_id.is_empty() || options.attempt_id.is_empty() {
        return Err(invalid("job_id and attempt_id must be non-empty"));
    }
    Uuid::parse_str(&options.transaction_uuid)
        .map_err(|_| invalid("transaction_uuid must be a UUID"))?;
    if options.rows_assigned == 0 {
        return Err(invalid(
            "a fragment publication must assign at least one row",
        ));
    }
    Ok(())
}

async fn latest_dataset(native: &NativeTable) -> Result<Dataset> {
    let mut dataset = (*native.dataset.get().await?).clone();
    dataset.checkout_latest().await?;
    Ok(dataset)
}

async fn capture_basis_from_dataset(
    dataset: &Dataset,
    binding_id: &str,
    fragment_id: u64,
) -> Result<FragmentPublicationBasis> {
    let arrow_schema = ArrowSchema::from(dataset.schema());
    ensure_supported_function_metadata(&arrow_schema)?;
    let binding = binding(&arrow_schema, binding_id)?;
    let fragment = fragment(dataset, fragment_id)?;
    let physical_rows = fragment.physical_rows().await? as u64;
    let fragment_digest = digest(fragment.metadata())?;
    let deletion_state_digest = digest(&fragment.metadata().deletion_file)?;
    let input_schema = binding.input_schema().cloned().ok_or_else(|| {
        invalid(format!(
            "Function binding '{}' has no exact input schema",
            binding.binding_id()
        ))
    })?;
    let output_schema = binding.output_schema().cloned().ok_or_else(|| {
        invalid(format!(
            "Function binding '{}' has no exact output schema",
            binding.binding_id()
        ))
    })?;
    let input_bindings = binding
        .inputs()
        .iter()
        .map(|input| FragmentInputBinding {
            parameter: input.parameter.clone(),
            field_path: input.field_path.clone(),
            arrow_type: input.arrow_type.clone(),
            nullable: input.nullable,
        })
        .collect();
    let output_bindings = binding
        .outputs()
        .iter()
        .map(|output| FragmentOutputBinding {
            result_field: output.result_field.clone(),
            field_path: output.output_name.clone(),
            output_ordinal: output.output_ordinal,
            arrow_type: output.arrow_type.clone(),
            nullable: output.nullable,
        })
        .collect();
    let mut basis = FragmentPublicationBasis {
        source_version: dataset.version().version,
        fragment_id,
        physical_rows,
        deletion_state_digest,
        fragment_digest,
        binding_id: binding.binding_id().to_string(),
        binding_revision: binding.revision(),
        input_bindings,
        output_bindings,
        input_schema,
        output_schema,
        basis_digest: String::new(),
    };
    basis.basis_digest = basis_digest(&basis)?;
    Ok(basis)
}

fn binding(schema: &ArrowSchema, binding_id: &str) -> Result<FunctionBinding> {
    function_bindings(schema)?
        .into_iter()
        .find(|binding| binding.binding_id() == binding_id)
        .ok_or_else(|| invalid(format!("Function binding '{binding_id}' is missing")))
}

fn fragment(dataset: &Dataset, fragment_id: u64) -> Result<FileFragment> {
    dataset
        .get_fragments()
        .into_iter()
        .find(|fragment| fragment.metadata().id == fragment_id)
        .ok_or_else(|| {
            invalid(format!(
                "fragment {fragment_id} is missing or was rewritten"
            ))
        })
}

async fn validate_basis(dataset: &Dataset, basis: &FragmentPublicationBasis) -> Result<()> {
    if basis.basis_digest != basis_digest(basis)? {
        return Err(invalid("fragment publication basis digest is invalid"));
    }
    if dataset.version().version < basis.source_version {
        return Err(invalid(format!(
            "table version {} predates publication source version {}",
            dataset.version().version,
            basis.source_version
        )));
    }
    let current = capture_basis_from_dataset(dataset, &basis.binding_id, basis.fragment_id).await?;
    if current.binding_revision != basis.binding_revision
        || current.input_bindings != basis.input_bindings
        || current.output_bindings != basis.output_bindings
        || current.input_schema != basis.input_schema
        || current.output_schema != basis.output_schema
    {
        return Err(invalid(format!(
            "Function binding '{}' changed after source version {}",
            basis.binding_id, basis.source_version
        )));
    }
    if current.physical_rows != basis.physical_rows
        || current.fragment_digest != basis.fragment_digest
        || current.deletion_state_digest != basis.deletion_state_digest
    {
        return Err(invalid(format!(
            "fragment {} row layout, source files, or deletion state changed after source version {}",
            basis.fragment_id, basis.source_version
        )));
    }
    Ok(())
}

fn expected_output_schema(basis: &FragmentPublicationBasis) -> Result<Arc<ArrowSchema>> {
    let json: JsonArrowSchema = serde_json::from_value(basis.output_schema.clone())
        .map_err(|error| invalid(format!("invalid binding output schema: {error}")))?;
    let schema = lance_namespace::schema::convert_json_arrow_schema(&json)
        .map_err(|error| invalid(format!("invalid binding output schema: {error}")))?;
    Ok(Arc::new(schema))
}

async fn validate_complete_output(
    dataset: &Dataset,
    basis: &FragmentPublicationBasis,
    expected_schema: &Arc<ArrowSchema>,
    output_batches: Vec<RecordBatch>,
    rows_assigned: u64,
) -> Result<Vec<RecordBatch>> {
    if output_batches.is_empty() {
        return Err(invalid(
            "complete fragment output requires at least one batch",
        ));
    }
    for batch in &output_batches {
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Err(invalid(
                "fragment output Arrow schema or nullability does not match the binding",
            ));
        }
    }
    let rows = output_batches
        .iter()
        .try_fold(0u64, |rows, batch| {
            rows.checked_add(batch.num_rows() as u64)
        })
        .ok_or_else(|| invalid("fragment output row count overflowed"))?;
    if rows != basis.physical_rows {
        return Err(invalid(format!(
            "fragment output covers {rows} rows, expected exactly {}",
            basis.physical_rows
        )));
    }

    let live = validate_unassigned_rows(dataset, basis, rows_assigned).await?;
    let mut physical_offset = 0;
    for batch in &output_batches {
        for (column_index, output) in basis.output_bindings.iter().enumerate() {
            let array = batch.column(column_index);
            if (0..batch.num_rows())
                .any(|offset| live[physical_offset + offset] && array.is_null(offset))
            {
                return Err(invalid(format!(
                    "Function output '{}' contains NULL for an assigned live row",
                    output.field_path
                )));
            }
        }
        physical_offset += batch.num_rows();
    }
    Ok(output_batches)
}

async fn validate_unassigned_rows(
    dataset: &Dataset,
    basis: &FragmentPublicationBasis,
    rows_assigned: u64,
) -> Result<Vec<bool>> {
    let fragment = fragment(dataset, basis.fragment_id)?;
    let output_paths = basis.output_field_paths();
    let mut scanner = dataset.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_id()
        .include_deleted_rows()
        .project(&output_paths)?;

    let mut live = Vec::with_capacity(basis.physical_rows as usize);
    let mut batches = scanner.try_into_stream().await?;
    while let Some(batch) = batches.try_next().await? {
        let row_ids = batch.column_by_name(ROW_ID).ok_or_else(|| {
            invalid("fragment assignment validation did not read physical row ids")
        })?;
        for offset in 0..batch.num_rows() {
            let is_live = !row_ids.is_null(offset);
            if is_live {
                for output in &basis.output_bindings {
                    let values = batch.column_by_name(&output.field_path).ok_or_else(|| {
                        invalid(format!(
                            "fragment assignment validation did not read output '{}'",
                            output.field_path
                        ))
                    })?;
                    if !values.is_null(offset) {
                        return Err(invalid(format!(
                            "fragment {} output group is already assigned or incomplete",
                            basis.fragment_id
                        )));
                    }
                }
            }
            live.push(is_live);
        }
    }
    if live.len() as u64 != basis.physical_rows {
        return Err(invalid(format!(
            "fragment {} read {} physical rows, expected {}",
            basis.fragment_id,
            live.len(),
            basis.physical_rows
        )));
    }
    let current_rows_assigned = live.iter().filter(|live| **live).count() as u64;
    if current_rows_assigned != rows_assigned {
        return Err(invalid(format!(
            "rows_assigned is {rows_assigned}, but fragment {} has {current_rows_assigned} unassigned live rows",
            basis.fragment_id
        )));
    }
    Ok(live)
}

fn output_lance_schema(dataset: &Dataset, basis: &FragmentPublicationBasis) -> Result<LanceSchema> {
    let mut fields = Vec::with_capacity(basis.output_bindings.len());
    for output in &basis.output_bindings {
        let field = dataset.schema().field(&output.field_path).ok_or_else(|| {
            invalid(format!(
                "Function output '{}' is missing from the table schema",
                output.field_path
            ))
        })?;
        if field.name != output.field_path {
            return Err(invalid(format!(
                "Function output '{}' is not a complete top-level column",
                output.field_path
            )));
        }
        fields.push(field.clone());
    }
    Ok(LanceSchema {
        fields,
        metadata: Default::default(),
    })
}

pub(super) async fn stage_fragment_columns(
    fragment: &FileFragment,
    data: impl Stream<Item = lance_core::Result<RecordBatch>> + Send,
    schema: &LanceSchema,
) -> Result<DataReplacementGroup> {
    Ok(fragment.write_columns(data, schema).await?)
}

fn transaction_properties(options: &FragmentPublicationOptions) -> Result<HashMap<String, String>> {
    Ok(HashMap::from([
        (TX_KIND_KEY.to_string(), TX_KIND.to_string()),
        (TX_JOB_KEY.to_string(), options.job_id.clone()),
        (TX_ATTEMPT_KEY.to_string(), options.attempt_id.clone()),
        (TX_BASIS_KEY.to_string(), options.basis.basis_digest.clone()),
        (
            TX_FRAGMENT_KEY.to_string(),
            options.basis.fragment_id.to_string(),
        ),
        (
            TX_OUTPUTS_KEY.to_string(),
            serde_json::to_string(&options.basis.output_field_paths()).map_err(|error| {
                invalid(format!("could not encode output field paths: {error}"))
            })?,
        ),
    ]))
}

async fn read_outcome(
    native: &NativeTable,
    basis: &FragmentPublicationBasis,
    transaction_uuid: &str,
) -> (CommitOutcome, Option<u64>) {
    if Uuid::parse_str(transaction_uuid).is_err()
        || basis_digest(basis).ok().as_deref() != Some(basis.basis_digest.as_str())
    {
        return (CommitOutcome::Unknown, None);
    }
    let Ok(dataset) = latest_dataset(native).await else {
        return (CommitOutcome::Unknown, None);
    };
    let latest_version = dataset.version().version;
    if latest_version <= basis.source_version {
        return (CommitOutcome::Absent, None);
    }
    let Ok(output_field_ids) = output_field_ids(&dataset, basis) else {
        return (CommitOutcome::Unknown, None);
    };

    let mut foreign = None;
    let mut unknown = false;
    for version in (basis.source_version + 1)..=latest_version {
        let transaction = match dataset.read_version_transaction(version).await {
            Ok(record) => match record.transaction {
                Some(transaction) => transaction,
                None => {
                    unknown = true;
                    continue;
                }
            },
            Err(_) => {
                unknown = true;
                continue;
            }
        };
        if transaction.uuid == transaction_uuid {
            return (CommitOutcome::Committed, Some(version));
        }
        if foreign.is_none()
            && overlaps_output_group(&transaction.operation, basis.fragment_id, &output_field_ids)
        {
            foreign = Some(CommitOutcome::Foreign {
                transaction_uuid: transaction.uuid,
                published_version: version,
            });
        }
    }
    let outcome = finish_readback(foreign, unknown);
    let version = match &outcome {
        CommitOutcome::Foreign {
            published_version, ..
        } => Some(*published_version),
        CommitOutcome::Committed | CommitOutcome::Absent | CommitOutcome::Unknown => None,
    };
    (outcome, version)
}

fn finish_readback(foreign: Option<CommitOutcome>, unknown: bool) -> CommitOutcome {
    foreign.unwrap_or(if unknown {
        CommitOutcome::Unknown
    } else {
        CommitOutcome::Absent
    })
}

fn output_field_ids(dataset: &Dataset, basis: &FragmentPublicationBasis) -> Result<BTreeSet<i32>> {
    // `DataReplacement` records physical fields by Lance's manifest IDs. They
    // are derived only to inspect that operation; no ID enters the V1 binding,
    // basis digest, validation gate, or receipt.
    basis
        .output_bindings
        .iter()
        .map(|output| {
            dataset
                .schema()
                .field(&output.field_path)
                .map(|field| field.id)
                .ok_or_else(|| {
                    invalid(format!(
                        "Function output '{}' is missing during outcome read-back",
                        output.field_path
                    ))
                })
        })
        .collect()
}

fn overlaps_output_group(
    operation: &Operation,
    fragment_id: u64,
    output_field_ids: &BTreeSet<i32>,
) -> bool {
    let Operation::DataReplacement { replacements } = operation else {
        return false;
    };
    replacements.iter().any(|replacement| {
        replacement.0 == fragment_id
            && replacement
                .1
                .fields
                .iter()
                .any(|field| output_field_ids.contains(field))
    })
}

fn receipt(
    options: &FragmentPublicationOptions,
    published_version: Option<u64>,
    commit_outcome: CommitOutcome,
) -> CommitReceipt {
    CommitReceipt {
        job_id: options.job_id.clone(),
        attempt_id: options.attempt_id.clone(),
        fragment_id: options.basis.fragment_id,
        basis_digest: options.basis.basis_digest.clone(),
        transaction_uuid: options.transaction_uuid.clone(),
        output_field_paths: options.basis.output_field_paths(),
        published_version,
        rows_assigned: options.rows_assigned,
        commit_outcome,
    }
}

fn basis_digest(basis: &FragmentPublicationBasis) -> Result<String> {
    #[derive(Serialize)]
    struct BasisDigest<'a> {
        source_version: u64,
        fragment_id: u64,
        physical_rows: u64,
        deletion_state_digest: &'a str,
        fragment_digest: &'a str,
        binding_id: &'a str,
        binding_revision: u64,
        input_bindings: &'a [FragmentInputBinding],
        output_bindings: &'a [FragmentOutputBinding],
        input_schema: &'a Value,
        output_schema: &'a Value,
    }
    digest(&BasisDigest {
        source_version: basis.source_version,
        fragment_id: basis.fragment_id,
        physical_rows: basis.physical_rows,
        deletion_state_digest: &basis.deletion_state_digest,
        fragment_digest: &basis.fragment_digest,
        binding_id: &basis.binding_id,
        binding_revision: basis.binding_revision,
        input_bindings: &basis.input_bindings,
        output_bindings: &basis.output_bindings,
        input_schema: &basis.input_schema,
        output_schema: &basis.output_schema,
    })
}

fn digest(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| invalid(format!("could not encode publication witness: {error}")))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::TryStreamExt;
    use lance::dataset::NewColumnTransform;

    use super::*;
    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::computed_columns::{
        COMPUTED_COLUMN_META_KEY, FUNCTION_BINDING_ID_META_KEY, FUNCTION_BINDINGS_META_KEY,
        FUNCTION_KIND, FUNCTION_OUTPUT_ORDINAL_META_KEY, INPUTS_META_KEY, KIND_META_KEY,
        function_bindings_metadata,
    };

    async fn function_table(name: &str) -> Table {
        let connection = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("title", DataType::Utf8, true),
            ArrowField::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])),
                Arc::new(StringArray::from(vec![Some("aa"), Some("bb"), Some("cc")])),
            ],
        )
        .unwrap();
        let table = connection
            .create_table(name, batch)
            .execute()
            .await
            .unwrap();

        let binding = FunctionBinding::from_json(include_str!(
            "../../tests/fixtures/first_class_functions/v1/remote_function_binding.json"
        ))
        .unwrap();
        let binding_metadata = function_bindings_metadata(&[binding]).unwrap();
        let output_fields = vec![
            function_field("search_text", DataType::Utf8, "fb_01K3TEXT", 0),
            function_field("search_token_count", DataType::Int64, "fb_01K3TEXT", 1),
        ];
        let output_schema = ArrowSchema::new_with_metadata(
            output_fields,
            HashMap::from([(FUNCTION_BINDINGS_META_KEY.to_string(), binding_metadata)]),
        );
        let native = table.as_native().unwrap();
        let mut dataset = (*native.dataset.get().await.unwrap()).clone();
        dataset
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(output_schema)),
                None,
                None,
            )
            .await
            .unwrap();
        native.dataset.update(dataset);
        table
    }

    fn function_field(
        name: &str,
        data_type: DataType,
        binding_id: &str,
        ordinal: u32,
    ) -> ArrowField {
        ArrowField::new(name, data_type, true).with_metadata(HashMap::from([
            (COMPUTED_COLUMN_META_KEY.to_string(), "true".to_string()),
            (KIND_META_KEY.to_string(), FUNCTION_KIND.to_string()),
            (
                FUNCTION_BINDING_ID_META_KEY.to_string(),
                binding_id.to_string(),
            ),
            (
                FUNCTION_OUTPUT_ORDINAL_META_KEY.to_string(),
                ordinal.to_string(),
            ),
            (
                INPUTS_META_KEY.to_string(),
                r#"["title","body"]"#.to_string(),
            ),
        ]))
    }

    fn output_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("search_text", DataType::Utf8, true),
            ArrowField::new("search_token_count", DataType::Int64, true),
        ]))
    }

    fn output_batch(prefix: &str) -> RecordBatch {
        RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some(format!("{prefix}a")),
                    Some(format!("{prefix}b")),
                    Some(format!("{prefix}c")),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .unwrap()
    }

    async fn output_values(table: &Table) -> (Vec<Option<String>>, Vec<Option<i64>>) {
        let batches = table
            .query()
            .select(Select::columns(&["search_text", "search_token_count"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let text = batches
            .iter()
            .flat_map(|batch| {
                batch["search_text"]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.map(ToString::to_string))
                    .collect::<Vec<_>>()
            })
            .collect();
        let counts = batches
            .iter()
            .flat_map(|batch| {
                batch["search_token_count"]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        (text, counts)
    }

    #[tokio::test]
    async fn publishes_complete_siblings_with_one_transaction_and_receipt() {
        let table = function_table("fragment_publish_complete").await;
        let publisher = FragmentPublisher::new(table.clone());
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();
        assert_eq!(basis.physical_rows(), 3);
        assert_eq!(basis.binding_revision(), 3);
        assert_eq!(basis.input_bindings().len(), 2);
        assert_eq!(
            basis.output_field_paths(),
            vec!["search_text", "search_token_count"]
        );
        let basis_json = serde_json::to_string(&basis).unwrap();
        assert!(!basis_json.contains("field_id"));
        assert!(basis.basis_digest().starts_with("sha256:"));
        assert!(
            basis
                .input_bindings()
                .iter()
                .all(|input| !input.field_path.is_empty())
        );

        let options = FragmentPublicationOptions::new("job-1", "attempt-1", basis.clone(), 3);
        let transaction_uuid = options.transaction_uuid.clone();
        let output = output_batch("published-");
        let receipt = publisher
            .publish(options, vec![output.slice(0, 1), output.slice(1, 2)])
            .await
            .unwrap();

        assert_eq!(receipt.job_id, "job-1");
        assert_eq!(receipt.attempt_id, "attempt-1");
        assert_eq!(receipt.fragment_id, 0);
        assert_eq!(receipt.basis_digest, basis.basis_digest());
        assert_eq!(receipt.transaction_uuid, transaction_uuid);
        assert_eq!(receipt.rows_assigned, 3);
        assert_eq!(receipt.commit_outcome, CommitOutcome::Committed);
        let version = receipt.published_version.unwrap();
        assert_eq!(
            publisher
                .read_commit_outcome(&basis, &transaction_uuid)
                .await,
            CommitOutcome::Committed
        );

        let native = table.as_native().unwrap();
        let dataset = native.dataset.get().await.unwrap();
        let transaction = dataset
            .read_version_transaction(version)
            .await
            .unwrap()
            .transaction
            .unwrap();
        assert_eq!(transaction.uuid, transaction_uuid);
        let properties = transaction.transaction_properties.as_ref().unwrap();
        assert_eq!(
            properties.get(TX_KIND_KEY).map(String::as_str),
            Some(TX_KIND)
        );
        assert_eq!(
            properties.get(TX_JOB_KEY).map(String::as_str),
            Some("job-1")
        );
        assert_eq!(
            properties.get(TX_ATTEMPT_KEY).map(String::as_str),
            Some("attempt-1")
        );
        assert!(matches!(
            transaction.operation,
            Operation::DataReplacement { replacements } if replacements.len() == 1
        ));
        assert_eq!(
            output_values(&table).await,
            (
                vec![
                    Some("published-a".to_string()),
                    Some("published-b".to_string()),
                    Some("published-c".to_string())
                ],
                vec![Some(1), Some(2), Some(3)]
            )
        );
    }

    #[tokio::test]
    async fn rejects_incomplete_group_coverage_schema_and_nulls_before_commit() {
        let table = function_table("fragment_publish_invalid_output").await;
        let publisher = FragmentPublisher::new(table.clone());
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();
        let source_version = basis.source_version();

        let short = RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "short", basis.clone(), 3),
                vec![short],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expected exactly 3"));

        let missing_sibling_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "search_text",
            DataType::Utf8,
            true,
        )]));
        let missing_sibling = RecordBatch::try_new(
            missing_sibling_schema,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "sibling", basis.clone(), 3),
                vec![missing_sibling],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("schema or nullability"));

        let null_output = RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
            ],
        )
        .unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "null", basis, 3),
                vec![null_output],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("contains NULL"));
        assert_eq!(table.version().await.unwrap(), source_version);
        assert_eq!(
            output_values(&table).await,
            (vec![None, None, None], vec![None, None, None])
        );
    }

    #[tokio::test]
    async fn rejects_binding_fragment_and_deletion_drift_before_commit() {
        let table = function_table("fragment_publish_basis_drift").await;
        let publisher = FragmentPublisher::new(table.clone());
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();

        let mut binding_drift = basis.clone();
        binding_drift.binding_revision += 1;
        binding_drift.basis_digest = basis_digest(&binding_drift).unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "binding", binding_drift, 3),
                vec![output_batch("binding-")],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("binding"));

        let mut fragment_drift = basis.clone();
        fragment_drift.fragment_digest = Uuid::new_v4().hyphenated().to_string();
        fragment_drift.basis_digest = basis_digest(&fragment_drift).unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "fragment", fragment_drift, 3),
                vec![output_batch("fragment-")],
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("row layout, source files, or deletion state")
        );

        table.delete("title = 'b'").await.unwrap();
        let err = publisher
            .publish(
                FragmentPublicationOptions::new("job", "deletion", basis, 3),
                vec![output_batch("deletion-")],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deletion state"));
        assert_eq!(
            output_values(&table).await,
            (vec![None, None], vec![None, None])
        );
    }

    #[tokio::test]
    async fn publishes_against_a_stable_deletion_witness_and_excludes_deleted_rows() {
        let table = function_table("fragment_publish_stable_deletion").await;
        table.delete("title = 'b'").await.unwrap();
        let publisher = FragmentPublisher::new(table.clone());
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();
        assert_ne!(
            basis.deletion_state_digest(),
            digest(&None::<Value>).unwrap()
        );

        let output = RecordBatch::try_new(
            output_schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("ready-a"),
                    None,
                    Some("ready-c"),
                ])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            ],
        )
        .unwrap();
        let receipt = publisher
            .publish(
                FragmentPublicationOptions::new("job", "deleted", basis, 2),
                vec![output],
            )
            .await
            .unwrap();
        assert_eq!(receipt.commit_outcome, CommitOutcome::Committed);
        assert_eq!(receipt.rows_assigned, 2);
        assert_eq!(
            output_values(&table).await,
            (
                vec![Some("ready-a".to_string()), Some("ready-c".to_string())],
                vec![Some(1), Some(3)]
            )
        );
    }

    #[tokio::test]
    async fn overlapping_attempts_commit_at_most_one_and_read_back_foreign() {
        let table = function_table("fragment_publish_overlap").await;
        let publisher = FragmentPublisher::new(table.clone());
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();
        let source_version = basis.source_version();
        let first = FragmentPublicationOptions::new("job", "first", basis.clone(), 3);
        let second = FragmentPublicationOptions::new("job", "second", basis.clone(), 3);
        let first_uuid = first.transaction_uuid.clone();
        let second_uuid = second.transaction_uuid.clone();

        let (first_result, second_result) = tokio::join!(
            publisher.publish(first, vec![output_batch("first-")]),
            publisher.publish(second, vec![output_batch("second-")])
        );
        let committed = [&first_result, &second_result]
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Ok(CommitReceipt {
                        commit_outcome: CommitOutcome::Committed,
                        ..
                    })
                )
            })
            .count();
        assert_eq!(committed, 1);
        assert_eq!(table.version().await.unwrap(), source_version + 1);

        let first_outcome = publisher.read_commit_outcome(&basis, &first_uuid).await;
        let second_outcome = publisher.read_commit_outcome(&basis, &second_uuid).await;
        assert!(matches!(
            (&first_outcome, &second_outcome),
            (CommitOutcome::Committed, CommitOutcome::Foreign { .. })
                | (CommitOutcome::Foreign { .. }, CommitOutcome::Committed)
        ));
        let absent = publisher
            .read_commit_outcome(&basis, &Uuid::new_v4().hyphenated().to_string())
            .await;
        assert!(matches!(absent, CommitOutcome::Foreign { .. }));

        let values = output_values(&table).await.0;
        assert!(
            values
                == vec![
                    Some("first-a".to_string()),
                    Some("first-b".to_string()),
                    Some("first-c".to_string())
                ]
                || values
                    == vec![
                        Some("second-a".to_string()),
                        Some("second-b".to_string()),
                        Some("second-c".to_string())
                    ]
        );
    }

    #[tokio::test]
    async fn readback_reports_absent_before_any_publication() {
        let table = function_table("fragment_publish_absent").await;
        let publisher = FragmentPublisher::new(table);
        let basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();
        assert_eq!(
            publisher
                .read_commit_outcome(&basis, &Uuid::new_v4().hyphenated().to_string(),)
                .await,
            CommitOutcome::Absent
        );
    }

    #[tokio::test]
    async fn compatible_append_survives_and_later_fragment_failure_does_not_roll_back() {
        let table = function_table("fragment_publish_append_then_failure").await;
        let publisher = FragmentPublisher::new(table.clone());
        let first_basis = publisher.capture_basis("fb_01K3TEXT", 0).await.unwrap();

        let append_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("title", DataType::Utf8, true),
            ArrowField::new("body", DataType::Utf8, true),
        ]));
        let append = RecordBatch::try_new(
            append_schema,
            vec![
                Arc::new(StringArray::from(vec![Some("d")])),
                Arc::new(StringArray::from(vec![Some("dd")])),
            ],
        )
        .unwrap();
        table.add(append).execute().await.unwrap();

        let first = publisher
            .publish(
                FragmentPublicationOptions::new("job", "first", first_basis, 3),
                vec![output_batch("kept-")],
            )
            .await
            .unwrap();
        assert_eq!(first.commit_outcome, CommitOutcome::Committed);

        let second_basis = publisher.capture_basis("fb_01K3TEXT", 1).await.unwrap();
        let incomplete = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "search_text",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("lost-d")]))],
        )
        .unwrap();
        assert!(
            publisher
                .publish(
                    FragmentPublicationOptions::new("job", "second", second_basis, 1),
                    vec![incomplete],
                )
                .await
                .is_err()
        );
        assert_eq!(
            output_values(&table).await,
            (
                vec![
                    Some("kept-a".to_string()),
                    Some("kept-b".to_string()),
                    Some("kept-c".to_string()),
                    None,
                ],
                vec![Some(1), Some(2), Some(3), None]
            )
        );
    }

    #[test]
    fn readback_keeps_missing_or_unreadable_history_unknown() {
        assert_eq!(finish_readback(None, true), CommitOutcome::Unknown);
        assert_eq!(finish_readback(None, false), CommitOutcome::Absent);
        let foreign = CommitOutcome::Foreign {
            transaction_uuid: "foreign".to_string(),
            published_version: 7,
        };
        assert_eq!(
            finish_readback(Some(foreign.clone()), true),
            foreign,
            "directly observed overlap remains foreign even if another version is unreadable"
        );
    }
}
