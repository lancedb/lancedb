// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/// Polars and LanceDB both use Arrow for their in memory-representation, but use
/// different Rust Arrow implementations. LanceDB uses the arrow-rs crate and
/// Polars uses the polars-arrow crate.
///
/// This crate defines zero-copy conversions (of the underlying buffers)
/// between polars-arrow and arrow-rs using the C FFI.
///
/// The polars-arrow does implement conversions to and from arrow-rs, but
/// requires a feature flagged dependency on arrow-rs. The version of arrow-rs
/// depended on by polars-arrow and LanceDB may not be compatible,
/// which necessitates using the C FFI.
use std::{mem, sync::Arc};

use polars::prelude::{DataFrame, Series, SchemaExt, CompatLevel};

use crate::error::Result;

/// When interpreting Polars dataframes as polars-arrow record batches,
/// one must decide whether to use Arrow string/binary view types
/// instead of the standard Arrow string/binary types.
/// For now, we will not use string view types because conversions
/// for string view types from polars-arrow to arrow-rs are not yet implemented.
/// See: https://lists.apache.org/thread/w88tpz76ox8h3rxkjl4so6rg3f1rv7wt for the
/// differences in the types.
pub const COMPAT_LEVEL:CompatLevel = CompatLevel::newest();
const IS_ARRAY_NULLABLE: bool = true;

/// Converts a Polars DataFrame schema to an Arrow RecordBatch schema.
pub fn convert_polars_df_schema_to_arrow_rb_schema(
    polars_df_schema: &polars::prelude::SchemaRef,
) -> Result<Arc<arrow_schema::Schema> > {
    let arrow_fields: Result<Vec<arrow_schema::Field>> = polars_df_schema.iter()
        .map(|(name, df_dtype)| {
            let polars_arrow_dtype = df_dtype.to_arrow(CompatLevel::newest());
            let polars_field =
                polars_arrow::datatypes::Field::new(name.clone(), polars_arrow_dtype, IS_ARRAY_NULLABLE);
            convert_polars_arrow_field_to_arrow_rs_field(polars_field)
        })
        .collect();
    Ok(Arc::new(arrow_schema::Schema::new(arrow_fields?)))
}

/// Converts an Arrow RecordBatch schema to a Polars DataFrame schema.
pub fn convert_arrow_rb_schema_to_polars_df_schema(
    arrow_schema: &arrow_schema::Schema,
) -> Result<polars::prelude::Schema> {
    let polars_arrow_schema: Result<polars::prelude::ArrowSchema> =
        arrow_schema
            .fields()
            .iter()
            .map(|arrow_rs_field| {
                let polars_arrow_field =
                    convert_arrow_rs_field_to_polars_arrow_field(arrow_rs_field)?;
                Ok((arrow_rs_field.name().into(), polars_arrow_field))
            })
            .collect();
    Ok(polars::prelude::Schema::from_arrow_schema(
        &polars_arrow_schema?,
    ))
}

/// Converts an Arrow RecordBatch to a Polars DataFrame, using a provided Polars DataFrame schema.
pub fn convert_arrow_rb_to_polars_df(
    arrow_rb: &arrow::record_batch::RecordBatch,
    polars_schema: &polars::prelude::Schema,
) -> Result<DataFrame> {
    let mut columns: Vec<Series> = Vec::with_capacity(arrow_rb.num_columns());

    for (column, (name, polars_df_dtype)) in arrow_rb.columns().iter().zip(polars_schema.iter()) {
        let polars_arrow_dtype = polars_df_dtype.to_arrow(CompatLevel::newest());
        let polars_array =
            convert_arrow_rs_array_to_polars_arrow_array(column, polars_arrow_dtype)?;
        columns.push(Series::from_arrow(name.clone(), polars_array)?);
    }

    Ok(DataFrame::from_iter(columns))
}

/// Converts a polars-arrow Arrow array to an arrow-rs Arrow array.
pub fn convert_polars_arrow_array_to_arrow_rs_array(
    polars_array: Box<dyn polars_arrow::array::Array>,
    arrow_datatype: arrow_schema::DataType,
) -> std::result::Result<arrow_array::ArrayRef, arrow_schema::ArrowError> {
    let polars_c_array = polars_arrow::ffi::export_array_to_c(polars_array);
    // Safety: `polars_arrow::ffi::ArrowArray` has the same memory layout as `arrow::ffi::FFI_ArrowArray`.
    let arrow_c_array: arrow_data::ffi::FFI_ArrowArray = unsafe { mem::transmute(polars_c_array) };
    Ok(arrow_array::make_array(unsafe {
        arrow::ffi::from_ffi_and_data_type(arrow_c_array, arrow_datatype)
    }?))
}

/// Converts an arrow-rs Arrow array to a polars-arrow Arrow array.
fn convert_arrow_rs_array_to_polars_arrow_array(
    arrow_rs_array: &Arc<dyn arrow_array::Array>,
    polars_arrow_dtype: polars::datatypes::ArrowDataType,
) -> Result<Box<dyn polars_arrow::array::Array>> {
    let arrow_c_array = arrow::ffi::FFI_ArrowArray::new(&arrow_rs_array.to_data());
    // Safety: `polars_arrow::ffi::ArrowArray` has the same memory layout as `arrow::ffi::FFI_ArrowArray`.
    let polars_c_array: polars_arrow::ffi::ArrowArray = unsafe { mem::transmute(arrow_c_array) };
    Ok(unsafe { polars_arrow::ffi::import_array_from_c(polars_c_array, polars_arrow_dtype) }?)
}

fn convert_polars_arrow_field_to_arrow_rs_field(
    polars_arrow_field: polars_arrow::datatypes::Field,
) -> Result<arrow_schema::Field> {
    let polars_c_schema = polars_arrow::ffi::export_field_to_c(&polars_arrow_field);
    // Safety: `polars_arrow::ffi::ArrowSchema` has the same memory layout as `arrow::ffi::FFI_ArrowSchema`.
    let arrow_c_schema: arrow::ffi::FFI_ArrowSchema =
        unsafe { mem::transmute::<_, _>(polars_c_schema) };
    Ok(arrow_schema::Field::try_from(&arrow_c_schema)?)
}

fn convert_arrow_rs_field_to_polars_arrow_field(
    arrow_rs_field: &arrow_schema::Field,
) -> Result<polars_arrow::datatypes::Field> {
    let arrow_c_schema = arrow::ffi::FFI_ArrowSchema::try_from(arrow_rs_field)?;
    // Safety: `polars_arrow::ffi::ArrowSchema` has the same memory layout as `arrow::ffi::FFI_ArrowSchema`.
    let polars_c_schema: polars_arrow::ffi::ArrowSchema =
        unsafe { mem::transmute::<_, _>(arrow_c_schema) };
    Ok(unsafe { polars_arrow::ffi::import_field_from_c(&polars_c_schema) }?)
}
