// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Extend Arrow Functionality
//!
//! To improve Arrow-RS ergonomic

#![warn(clippy::undocumented_unsafe_blocks)]

// lance-arrow reinterprets value bytes as native numeric types in
// `FloatArray::as_slice` for `bf16` (rust/lance-arrow/src/bfloat16.rs), which
// requires the host byte order to match the on-disk byte order Lance writes.
// Lance writes little-endian; building on a big-endian target would silently
// produce wrong numeric values.
#[cfg(not(target_endian = "little"))]
compile_error!("lance-arrow only supports little-endian targets");

use std::sync::Arc;
use std::{collections::HashMap, ptr::NonNull};

use arrow_array::{
    Array, ArrayRef, ArrowNumericType, FixedSizeBinaryArray, FixedSizeListArray, GenericListArray,
    LargeListArray, ListArray, MapArray, OffsetSizeTrait, PrimitiveArray, RecordBatch, StructArray,
    UInt8Array, UInt32Array, cast::AsArray,
};
use arrow_array::{
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, new_null_array,
};
use arrow_buffer::{BooleanBufferBuilder, MutableBuffer, NullBuffer};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, Field, Fields, IntervalUnit, Schema, SortOptions};
use arrow_select::{interleave::interleave, take::take};
use rand::prelude::*;

pub mod deepcopy;
pub mod schema;
pub use schema::*;
pub mod bfloat16;
pub mod floats;
use crate::list::ListArrayExt;
pub use floats::*;

pub mod ipc;
pub mod json;
pub mod list;
pub mod memory;
pub mod scalar;
pub mod stream;
pub mod r#struct;

/// Arrow extension metadata key for extension name
pub const ARROW_EXT_NAME_KEY: &str = "ARROW:extension:name";

/// Arrow extension metadata key for extension metadata
pub const ARROW_EXT_META_KEY: &str = "ARROW:extension:metadata";

/// Key used by lance to mark a field as a blob
/// TODO: Use Arrow extension mechanism instead?
pub const BLOB_META_KEY: &str = "lance-encoding:blob";
/// Arrow extension type name for Lance blob v2 columns
pub const BLOB_V2_EXT_NAME: &str = "lance.blob.v2";
/// Metadata key for overriding the dedicated blob size threshold (in bytes)
pub const BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY: &str =
    "lance-encoding:blob-dedicated-size-threshold";
/// Metadata key for overriding the inline blob size threshold (in bytes)
pub const BLOB_INLINE_SIZE_THRESHOLD_META_KEY: &str = "lance-encoding:blob-inline-size-threshold";
/// Metadata key for overriding the maximum size (in bytes) of a packed blob sidecar file
pub const BLOB_PACK_FILE_SIZE_THRESHOLD_META_KEY: &str =
    "lance-encoding:blob-pack-file-size-threshold";

type Result<T> = std::result::Result<T, ArrowError>;

pub trait DataTypeExt {
    /// Returns true if the data type is binary-like, such as Utf8, Binary, or the large and/or view variants.
    fn is_binary_like(&self) -> bool;

    /// Returns true if the data type is a struct.
    fn is_struct(&self) -> bool;

    /// Check whether the given Arrow DataType is fixed stride.
    ///
    /// A fixed stride type has the same byte width for all array elements
    /// This includes all PrimitiveType's Boolean, FixedSizeList, FixedSizeBinary, and Decimals
    fn is_fixed_stride(&self) -> bool;

    /// Returns true if the [DataType] is a dictionary type.
    fn is_dictionary(&self) -> bool;

    /// Returns the byte width of the data type
    /// Panics if the data type is not fixed stride.
    fn byte_width(&self) -> usize;

    /// Returns the byte width of the data type, if it is fixed stride.
    /// Returns None if the data type is not fixed stride.
    fn byte_width_opt(&self) -> Option<usize>;
}

impl DataTypeExt for DataType {
    fn is_binary_like(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            Utf8 | Binary | LargeUtf8 | LargeBinary | Utf8View | BinaryView
        )
    }

    fn is_struct(&self) -> bool {
        matches!(self, Self::Struct(_))
    }

    fn is_fixed_stride(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            Boolean
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Int8
                | Int16
                | Int32
                | Int64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | FixedSizeList(_, _)
                | FixedSizeBinary(_)
                | Duration(_)
                | Timestamp(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
        )
    }

    fn is_dictionary(&self) -> bool {
        matches!(self, Self::Dictionary(_, _))
    }

    fn byte_width_opt(&self) -> Option<usize> {
        match self {
            Self::Int8 => Some(1),
            Self::Int16 => Some(2),
            Self::Int32 => Some(4),
            Self::Int64 => Some(8),
            Self::UInt8 => Some(1),
            Self::UInt16 => Some(2),
            Self::UInt32 => Some(4),
            Self::UInt64 => Some(8),
            Self::Float16 => Some(2),
            Self::Float32 => Some(4),
            Self::Float64 => Some(8),
            Self::Date32 => Some(4),
            Self::Date64 => Some(8),
            Self::Time32(_) => Some(4),
            Self::Time64(_) => Some(8),
            Self::Timestamp(_, _) => Some(8),
            Self::Duration(_) => Some(8),
            Self::Decimal128(_, _) => Some(16),
            Self::Decimal256(_, _) => Some(32),
            Self::Interval(unit) => match unit {
                IntervalUnit::YearMonth => Some(4),
                IntervalUnit::DayTime => Some(8),
                IntervalUnit::MonthDayNano => Some(16),
            },
            Self::FixedSizeBinary(s) => Some(*s as usize),
            Self::FixedSizeList(dt, s) => dt
                .data_type()
                .byte_width_opt()
                .map(|width| width * *s as usize),
            _ => None,
        }
    }

    fn byte_width(&self) -> usize {
        self.byte_width_opt()
            .unwrap_or_else(|| panic!("Expecting fixed stride data type, found {:?}", self))
    }
}

/// Create an [`GenericListArray`] from values and offsets.
///
/// ```
/// use arrow_array::{Int32Array, Int64Array, ListArray};
/// use arrow_array::types::Int64Type;
/// use lance_arrow::try_new_generic_list_array;
///
/// let offsets = Int32Array::from_iter([0, 2, 7, 10]);
/// let int_values = Int64Array::from_iter(0..10);
/// let list_arr = try_new_generic_list_array(int_values, &offsets).unwrap();
/// assert_eq!(list_arr,
///     ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
///         Some(vec![Some(0), Some(1)]),
///         Some(vec![Some(2), Some(3), Some(4), Some(5), Some(6)]),
///         Some(vec![Some(7), Some(8), Some(9)]),
/// ]))
/// ```
pub fn try_new_generic_list_array<T: Array, Offset: ArrowNumericType>(
    values: T,
    offsets: &PrimitiveArray<Offset>,
) -> Result<GenericListArray<Offset::Native>>
where
    Offset::Native: OffsetSizeTrait,
{
    let data_type = if Offset::Native::IS_LARGE {
        DataType::LargeList(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            true,
        )))
    } else {
        DataType::List(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            true,
        )))
    };
    let data = ArrayDataBuilder::new(data_type)
        .len(offsets.len() - 1)
        .add_buffer(offsets.into_data().buffers()[0].clone())
        .add_child_data(values.into_data())
        .build()?;

    Ok(GenericListArray::from(data))
}

pub fn fixed_size_list_type(list_width: i32, inner_type: DataType) -> DataType {
    DataType::FixedSizeList(Arc::new(Field::new("item", inner_type, true)), list_width)
}

pub trait FixedSizeListArrayExt {
    /// Create an [`FixedSizeListArray`] from values and list size.
    ///
    /// ```
    /// use arrow_array::{Int64Array, FixedSizeListArray};
    /// use arrow_array::types::Int64Type;
    /// use lance_arrow::FixedSizeListArrayExt;
    ///
    /// let int_values = Int64Array::from_iter(0..10);
    /// let fixed_size_list_arr = FixedSizeListArray::try_new_from_values(int_values, 2).unwrap();
    /// assert_eq!(fixed_size_list_arr,
    ///     FixedSizeListArray::from_iter_primitive::<Int64Type, _, _>(vec![
    ///         Some(vec![Some(0), Some(1)]),
    ///         Some(vec![Some(2), Some(3)]),
    ///         Some(vec![Some(4), Some(5)]),
    ///         Some(vec![Some(6), Some(7)]),
    ///         Some(vec![Some(8), Some(9)])
    /// ], 2))
    /// ```
    fn try_new_from_values<T: Array + 'static>(
        values: T,
        list_size: i32,
    ) -> Result<FixedSizeListArray>;

    /// Sample `n` rows from the [FixedSizeListArray]
    ///
    /// ```
    /// use arrow_array::{Int64Array, FixedSizeListArray, Array};
    /// use lance_arrow::FixedSizeListArrayExt;
    ///
    /// let int_values = Int64Array::from_iter(0..256);
    /// let fixed_size_list_arr = FixedSizeListArray::try_new_from_values(int_values, 16).unwrap();
    /// let sampled = fixed_size_list_arr.sample(10).unwrap();
    /// assert_eq!(sampled.len(), 10);
    /// assert_eq!(sampled.value_length(), 16);
    /// assert_eq!(sampled.values().len(), 160);
    /// ```
    fn sample(&self, n: usize) -> Result<FixedSizeListArray>;

    /// Ensure the [FixedSizeListArray] of Float16, Float32, Float64,
    /// Int8, Int16, Int32, Int64, UInt8, UInt32 type to its closest floating point type.
    fn convert_to_floating_point(&self) -> Result<FixedSizeListArray>;
}

impl FixedSizeListArrayExt for FixedSizeListArray {
    fn try_new_from_values<T: Array + 'static>(values: T, list_size: i32) -> Result<Self> {
        let field = Arc::new(Field::new("item", values.data_type().clone(), true));
        let values = Arc::new(values);

        Self::try_new(field, list_size, values, None)
    }

    fn sample(&self, n: usize) -> Result<FixedSizeListArray> {
        if n >= self.len() {
            return Ok(self.clone());
        }
        let mut rng = SmallRng::from_os_rng();
        let chosen = (0..self.len() as u32).choose_multiple(&mut rng, n);
        take(self, &UInt32Array::from(chosen), None).map(|arr| arr.as_fixed_size_list().clone())
    }

    fn convert_to_floating_point(&self) -> Result<FixedSizeListArray> {
        match self.data_type() {
            DataType::FixedSizeList(field, size) => match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(self.clone()),
                DataType::Int8 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float32,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float32Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to Int8Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f32)),
                    )),
                    self.nulls().cloned(),
                )),
                DataType::Int16 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float32,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float32Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<Int16Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to Int16Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f32)),
                    )),
                    self.nulls().cloned(),
                )),
                DataType::Int32 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float32,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float32Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to Int32Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f32)),
                    )),
                    self.nulls().cloned(),
                )),
                DataType::Int64 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float64,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float64Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to Int64Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f64)),
                    )),
                    self.nulls().cloned(),
                )),
                DataType::UInt8 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float64,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float64Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<UInt8Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to UInt8Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f64)),
                    )),
                    self.nulls().cloned(),
                )),
                DataType::UInt32 => Ok(Self::new(
                    Arc::new(arrow_schema::Field::new(
                        field.name(),
                        DataType::Float64,
                        field.is_nullable(),
                    )),
                    *size,
                    Arc::new(Float64Array::from_iter(
                        self.values()
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .ok_or(ArrowError::ParseError(
                                "Fail to cast primitive array to UInt32Type".to_string(),
                            ))?
                            .into_iter()
                            .map(|x| x.map(|y| y as f64)),
                    )),
                    self.nulls().cloned(),
                )),
                data_type => Err(ArrowError::ParseError(format!(
                    "Expect either floating type or integer got {:?}",
                    data_type
                ))),
            },
            data_type => Err(ArrowError::ParseError(format!(
                "Expect either FixedSizeList got {:?}",
                data_type
            ))),
        }
    }
}

/// Force downcast of an [`Array`], such as an [`ArrayRef`], to
/// [`FixedSizeListArray`], panic'ing on failure.
pub fn as_fixed_size_list_array(arr: &dyn Array) -> &FixedSizeListArray {
    arr.as_any().downcast_ref::<FixedSizeListArray>().unwrap()
}

pub trait FixedSizeBinaryArrayExt {
    /// Create an [`FixedSizeBinaryArray`] from values and stride.
    ///
    /// ```
    /// use arrow_array::{UInt8Array, FixedSizeBinaryArray};
    /// use arrow_array::types::UInt8Type;
    /// use lance_arrow::FixedSizeBinaryArrayExt;
    ///
    /// let int_values = UInt8Array::from_iter(0..10);
    /// let fixed_size_list_arr = FixedSizeBinaryArray::try_new_from_values(&int_values, 2).unwrap();
    /// assert_eq!(fixed_size_list_arr,
    ///     FixedSizeBinaryArray::try_from_iter(
    ///         [[0u8, 1], [2, 3], [4, 5], [6, 7], [8, 9]].into_iter()
    ///     ).unwrap())
    /// ```
    fn try_new_from_values(values: &UInt8Array, stride: i32) -> Result<FixedSizeBinaryArray>;
}

impl FixedSizeBinaryArrayExt for FixedSizeBinaryArray {
    fn try_new_from_values(values: &UInt8Array, stride: i32) -> Result<Self> {
        let data_type = DataType::FixedSizeBinary(stride);
        let data = ArrayDataBuilder::new(data_type)
            .len(values.len() / stride as usize)
            .add_buffer(values.into_data().buffers()[0].clone())
            .build()?;
        Ok(Self::from(data))
    }
}

pub fn as_fixed_size_binary_array(arr: &dyn Array) -> &FixedSizeBinaryArray {
    arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap()
}

pub fn iter_str_array(arr: &dyn Array) -> Box<dyn Iterator<Item = Option<&str>> + Send + '_> {
    match arr.data_type() {
        DataType::Utf8 => Box::new(arr.as_string::<i32>().iter()),
        DataType::LargeUtf8 => Box::new(arr.as_string::<i64>().iter()),
        DataType::Utf8View => Box::new(arr.as_string_view().iter()),
        _ => panic!(
            "Expecting Utf8, LargeUtf8 or Utf8View, found {:?}",
            arr.data_type()
        ),
    }
}

pub fn iter_binary_array(
    arr: &dyn Array,
) -> Result<Box<dyn Iterator<Item = Option<&[u8]>> + Send + '_>> {
    match arr.data_type() {
        DataType::Binary => Ok(Box::new(arr.as_binary::<i32>().iter())),
        DataType::LargeBinary => Ok(Box::new(arr.as_binary::<i64>().iter())),
        DataType::BinaryView => Ok(Box::new(arr.as_binary_view().iter())),
        DataType::FixedSizeBinary(_) => Ok(Box::new(arr.as_fixed_size_binary().iter())),
        data_type => Err(ArrowError::InvalidArgumentError(format!(
            "Expecting a binary type, found {data_type}"
        ))),
    }
}

/// Extends Arrow's [RecordBatch].
pub trait RecordBatchExt {
    /// Append a new column to this [`RecordBatch`] and returns a new RecordBatch.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::{RecordBatch, Int32Array, StringArray};
    /// use arrow_schema::{Schema, Field, DataType};
    /// use lance_arrow::*;
    ///
    /// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    /// let int_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    /// let record_batch = RecordBatch::try_new(schema, vec![int_arr.clone()]).unwrap();
    ///
    /// let new_field = Field::new("s", DataType::Utf8, true);
    /// let str_arr = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    /// let new_record_batch = record_batch.try_with_column(new_field, str_arr.clone()).unwrap();
    ///
    /// assert_eq!(
    ///     new_record_batch,
    ///     RecordBatch::try_new(
    ///         Arc::new(Schema::new(
    ///             vec![
    ///                 Field::new("a", DataType::Int32, true),
    ///                 Field::new("s", DataType::Utf8, true)
    ///             ])
    ///         ),
    ///         vec![int_arr, str_arr],
    ///     ).unwrap()
    /// )
    /// ```
    fn try_with_column(&self, field: Field, arr: ArrayRef) -> Result<RecordBatch>;

    /// Created a new RecordBatch with column at index.
    fn try_with_column_at(&self, index: usize, field: Field, arr: ArrayRef) -> Result<RecordBatch>;

    /// Creates a new [`RecordBatch`] from the provided  [`StructArray`].
    ///
    /// The fields on the [`StructArray`] need to match this [`RecordBatch`] schema
    fn try_new_from_struct_array(&self, arr: StructArray) -> Result<RecordBatch>;

    /// Merge with another [`RecordBatch`] and returns a new one.
    ///
    /// Fields are merged based on name.  First we iterate the left columns.  If a matching
    /// name is found in the right then we merge the two columns.  If there is no match then
    /// we add the left column to the output.
    ///
    /// To merge two columns we consider the type.  If both arrays are struct arrays we recurse.
    /// Otherwise we use the left array.
    ///
    /// Afterwards we add all non-matching right columns to the output.
    ///
    /// Note: This method likely does not handle nested fields correctly and you may want to consider
    /// using [`Self::merge_with_schema`] instead.
    /// ```
    /// use std::sync::Arc;
    /// use arrow_array::*;
    /// use arrow_schema::{Schema, Field, DataType};
    /// use lance_arrow::*;
    ///
    /// let left_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    /// let int_arr = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    /// let left = RecordBatch::try_new(left_schema, vec![int_arr.clone()]).unwrap();
    ///
    /// let right_schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    /// let str_arr = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    /// let right = RecordBatch::try_new(right_schema, vec![str_arr.clone()]).unwrap();
    ///
    /// let new_record_batch = left.merge(&right).unwrap();
    ///
    /// assert_eq!(
    ///     new_record_batch,
    ///     RecordBatch::try_new(
    ///         Arc::new(Schema::new(
    ///             vec![
    ///                 Field::new("a", DataType::Int32, true),
    ///                 Field::new("s", DataType::Utf8, true)
    ///             ])
    ///         ),
    ///         vec![int_arr, str_arr],
    ///     ).unwrap()
    /// )
    /// ```
    ///
    /// TODO: add merge nested fields support.
    fn merge(&self, other: &RecordBatch) -> Result<RecordBatch>;

    /// Create a batch by merging columns between two batches with a given schema.
    ///
    /// A reference schema is used to determine the proper ordering of nested fields.
    ///
    /// For each field in the reference schema we look for corresponding fields in
    /// the left and right batches.  If a field is found in both batches we recursively merge
    /// it.
    ///
    /// If a field is only in the left or right batch we take it as it is.
    fn merge_with_schema(&self, other: &RecordBatch, schema: &Schema) -> Result<RecordBatch>;

    /// Drop one column specified with the name and return the new [`RecordBatch`].
    ///
    /// If the named column does not exist, it returns a copy of this [`RecordBatch`].
    fn drop_column(&self, name: &str) -> Result<RecordBatch>;

    /// Replace a column (specified by name) and return the new [`RecordBatch`].
    fn replace_column_by_name(&self, name: &str, column: Arc<dyn Array>) -> Result<RecordBatch>;

    /// Replace a column schema (specified by name) and return the new [`RecordBatch`].
    fn replace_column_schema_by_name(
        &self,
        name: &str,
        new_data_type: DataType,
        column: Arc<dyn Array>,
    ) -> Result<RecordBatch>;

    /// Rename a column at a given index.
    fn rename_column(&self, index: usize, new_name: &str) -> Result<RecordBatch>;

    /// Get (potentially nested) column by qualified name.
    fn column_by_qualified_name(&self, name: &str) -> Option<&ArrayRef>;

    /// Project the schema over the [RecordBatch].
    fn project_by_schema(&self, schema: &Schema) -> Result<RecordBatch>;

    /// metadata of the schema.
    fn metadata(&self) -> &HashMap<String, String>;

    /// Add metadata to the schema.
    fn add_metadata(&self, key: String, value: String) -> Result<RecordBatch> {
        let mut metadata = self.metadata().clone();
        metadata.insert(key, value);
        self.with_metadata(metadata)
    }

    /// Replace the schema metadata with the provided one.
    fn with_metadata(&self, metadata: HashMap<String, String>) -> Result<RecordBatch>;

    /// Take selected rows from the [RecordBatch].
    fn take(&self, indices: &UInt32Array) -> Result<RecordBatch>;

    /// Create a new RecordBatch with compacted memory after slicing.
    fn shrink_to_fit(&self) -> Result<RecordBatch>;

    /// Helper method to sort the RecordBatch by a column
    fn sort_by_column(&self, column: usize, options: Option<SortOptions>) -> Result<RecordBatch>;
}

impl RecordBatchExt for RecordBatch {
    fn try_with_column(&self, field: Field, arr: ArrayRef) -> Result<Self> {
        let new_schema = Arc::new(self.schema().as_ref().try_with_column(field)?);
        let mut new_columns = self.columns().to_vec();
        new_columns.push(arr);
        Self::try_new(new_schema, new_columns)
    }

    fn try_with_column_at(&self, index: usize, field: Field, arr: ArrayRef) -> Result<Self> {
        let new_schema = Arc::new(self.schema().as_ref().try_with_column_at(index, field)?);
        let mut new_columns = self.columns().to_vec();
        new_columns.insert(index, arr);
        Self::try_new(new_schema, new_columns)
    }

    fn try_new_from_struct_array(&self, arr: StructArray) -> Result<Self> {
        let schema = Arc::new(Schema::new_with_metadata(
            arr.fields().to_vec(),
            self.schema().metadata.clone(),
        ));
        let batch = Self::from(arr);
        batch.with_schema(schema)
    }

    fn merge(&self, other: &Self) -> Result<Self> {
        if self.num_rows() != other.num_rows() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Attempt to merge two RecordBatch with different sizes: {} != {}",
                self.num_rows(),
                other.num_rows()
            )));
        }
        let left_struct_array: StructArray = self.clone().into();
        let right_struct_array: StructArray = other.clone().into();
        self.try_new_from_struct_array(merge(&left_struct_array, &right_struct_array)?)
    }

    fn merge_with_schema(&self, other: &RecordBatch, schema: &Schema) -> Result<RecordBatch> {
        if self.num_rows() != other.num_rows() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Attempt to merge two RecordBatch with different sizes: {} != {}",
                self.num_rows(),
                other.num_rows()
            )));
        }
        let left_struct_array: StructArray = self.clone().into();
        let right_struct_array: StructArray = other.clone().into();
        self.try_new_from_struct_array(merge_with_schema(
            &left_struct_array,
            &right_struct_array,
            schema.fields(),
        ))
    }

    fn drop_column(&self, name: &str) -> Result<Self> {
        let mut fields = vec![];
        let mut columns = vec![];
        for i in 0..self.schema().fields.len() {
            if self.schema().field(i).name() != name {
                fields.push(self.schema().field(i).clone());
                columns.push(self.column(i).clone());
            }
        }
        Self::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                self.schema().metadata().clone(),
            )),
            columns,
        )
    }

    fn rename_column(&self, index: usize, new_name: &str) -> Result<RecordBatch> {
        let mut fields = self.schema().fields().to_vec();
        if index >= fields.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Index out of bounds: {}",
                index
            )));
        }
        fields[index] = Arc::new(Field::new(
            new_name,
            fields[index].data_type().clone(),
            fields[index].is_nullable(),
        ));
        Self::try_new(
            Arc::new(Schema::new_with_metadata(
                fields,
                self.schema().metadata().clone(),
            )),
            self.columns().to_vec(),
        )
    }

    fn replace_column_by_name(&self, name: &str, column: Arc<dyn Array>) -> Result<RecordBatch> {
        let mut columns = self.columns().to_vec();
        let field_i = self
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| ArrowError::SchemaError(format!("Field {} does not exist", name)))?;
        columns[field_i] = column;
        Self::try_new(self.schema(), columns)
    }

    fn replace_column_schema_by_name(
        &self,
        name: &str,
        new_data_type: DataType,
        column: Arc<dyn Array>,
    ) -> Result<RecordBatch> {
        let fields = self
            .schema()
            .fields()
            .iter()
            .map(|x| {
                if x.name() != name {
                    x.clone()
                } else {
                    let new_field = Field::new(name, new_data_type.clone(), x.is_nullable());
                    Arc::new(new_field)
                }
            })
            .collect::<Vec<_>>();
        let schema = Schema::new_with_metadata(fields, self.schema().metadata.clone());
        let mut columns = self.columns().to_vec();
        let field_i = self
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| ArrowError::SchemaError(format!("Field {} does not exist", name)))?;
        columns[field_i] = column;
        Self::try_new(Arc::new(schema), columns)
    }

    fn column_by_qualified_name(&self, name: &str) -> Option<&ArrayRef> {
        let split = name.split('.').collect::<Vec<_>>();
        if split.is_empty() {
            return None;
        }

        self.column_by_name(split[0])
            .and_then(|arr| get_sub_array(arr, &split[1..]))
    }

    fn project_by_schema(&self, schema: &Schema) -> Result<Self> {
        let struct_array: StructArray = self.clone().into();
        self.try_new_from_struct_array(project(&struct_array, schema.fields())?)
    }

    fn metadata(&self) -> &HashMap<String, String> {
        self.schema_ref().metadata()
    }

    fn with_metadata(&self, metadata: HashMap<String, String>) -> Result<RecordBatch> {
        let mut schema = self.schema_ref().as_ref().clone();
        schema.metadata = metadata;
        Self::try_new(schema.into(), self.columns().into())
    }

    fn take(&self, indices: &UInt32Array) -> Result<Self> {
        let struct_array: StructArray = self.clone().into();
        let taken = take(&struct_array, indices, None)?;
        self.try_new_from_struct_array(taken.as_struct().clone())
    }

    fn shrink_to_fit(&self) -> Result<Self> {
        // Deep copy the sliced record batch, instead of whole batch
        crate::deepcopy::deep_copy_batch_sliced(self)
    }

    fn sort_by_column(&self, column: usize, options: Option<SortOptions>) -> Result<Self> {
        if column >= self.num_columns() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Column index out of bounds: {}",
                column
            )));
        }
        let column = self.column(column);
        let sorted = arrow_ord::sort::sort_to_indices(column, options, None)?;
        self.take(&sorted)
    }
}

/// Recursively projects an array to match the target field's structure.
/// This handles reordering fields inside nested List<Struct> types.
fn project_array(array: &ArrayRef, target_field: &Field) -> Result<ArrayRef> {
    match target_field.data_type() {
        DataType::Struct(subfields) => {
            let struct_arr = array.as_struct();
            let projected = project(struct_arr, subfields)?;
            Ok(Arc::new(projected))
        }
        DataType::List(inner_field) => {
            let list_arr: &ListArray = array.as_list();
            let projected_values = project_array(list_arr.values(), inner_field.as_ref())?;
            Ok(Arc::new(ListArray::new(
                inner_field.clone(),
                list_arr.offsets().clone(),
                projected_values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::LargeList(inner_field) => {
            let list_arr: &LargeListArray = array.as_list();
            let projected_values = project_array(list_arr.values(), inner_field.as_ref())?;
            Ok(Arc::new(LargeListArray::new(
                inner_field.clone(),
                list_arr.offsets().clone(),
                projected_values,
                list_arr.nulls().cloned(),
            )))
        }
        DataType::FixedSizeList(inner_field, size) => {
            let list_arr = array.as_fixed_size_list();
            let projected_values = project_array(list_arr.values(), inner_field.as_ref())?;
            Ok(Arc::new(FixedSizeListArray::new(
                inner_field.clone(),
                *size,
                projected_values,
                list_arr.nulls().cloned(),
            )))
        }
        // A nullable entries field fails MapArray::try_new unconditionally,
        // so a (schema-invalid) map declared that way keeps the clone
        // fallthrough it always had rather than gaining a new error.
        DataType::Map(entries_field, sorted) if !entries_field.is_nullable() => {
            let map_arr = array.as_map();
            let DataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(ArrowError::SchemaError(format!(
                    "Map entries field must be a struct, got {}",
                    entries_field.data_type()
                )));
            };
            let projected_entries = project(map_arr.entries(), entry_fields)?;
            // try_new re-checks the entries invariants (a non-null entries
            // struct, two entry columns, offset bounds); null keys are ruled
            // out one level down, by the struct rebuild against the
            // non-nullable key field.
            Ok(Arc::new(MapArray::try_new(
                entries_field.clone(),
                map_arr.offsets().clone(),
                projected_entries,
                map_arr.nulls().cloned(),
                *sorted,
            )?))
        }
        _ => Ok(array.clone()),
    }
}

fn project(struct_array: &StructArray, fields: &Fields) -> Result<StructArray> {
    if fields.is_empty() {
        return Ok(StructArray::new_empty_fields(
            struct_array.len(),
            struct_array.nulls().cloned(),
        ));
    }
    let mut columns: Vec<ArrayRef> = vec![];
    for field in fields.iter() {
        if let Some(col) = struct_array.column_by_name(field.name()) {
            let projected = project_array(col, field.as_ref())?;
            columns.push(projected);
        } else {
            return Err(ArrowError::SchemaError(format!(
                "field {} does not exist in the RecordBatch",
                field.name()
            )));
        }
    }
    // Preserve the struct's validity when projecting
    StructArray::try_new(fields.clone(), columns, struct_array.nulls().cloned())
}

/// Whether `data_type` has a struct somewhere below it, through lists.
fn holds_struct(data_type: &DataType) -> bool {
    match data_type {
        DataType::Struct(_) => true,
        DataType::List(item) | DataType::LargeList(item) => holds_struct(item.data_type()),
        _ => false,
    }
}

/// The values of `list`, with the items of every entry that is null in `list`
/// but valid in `other` made null. Those entries become valid when the two
/// lists are merged, and their items here were never meant to be read. Merging
/// the items then pushes these nulls into their children.
fn hide_items_revealed_by<T: OffsetSizeTrait>(
    list: &GenericListArray<T>,
    other: Option<&NullBuffer>,
) -> ArrayRef {
    let Some(nulls) = list.nulls() else {
        return list.values().clone();
    };
    let revealed = match other {
        Some(other) => other.inner() & &!nulls.inner(),
        None => !nulls.inner(),
    };
    if revealed.count_set_bits() == 0 {
        return list.values().clone();
    }
    let offsets = list.value_offsets();
    let mut items_validity = BooleanBufferBuilder::new(list.values().len());
    items_validity.append_n(list.values().len(), true);
    for row in revealed.set_indices() {
        for item in offsets[row].as_usize()..offsets[row + 1].as_usize() {
            items_validity.set_bit(item, false);
        }
    }
    adjust_child_validity(
        list.values(),
        Some(&NullBuffer::new(items_validity.finish())),
    )
}

/// Merge the values of two lists: structs field by field, nested lists
/// recursively, anything else keeps the left side.
fn merge_list_values(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    Ok(match (left.data_type(), right.data_type()) {
        (DataType::Struct(_), DataType::Struct(_)) => {
            Arc::new(merge(left.as_struct(), right.as_struct())?)
        }
        (DataType::List(_), DataType::List(_))
        | (DataType::LargeList(_), DataType::LargeList(_))
            if left.data_type() != right.data_type()
                && holds_struct(left.data_type())
                && holds_struct(right.data_type()) =>
        {
            merge_list_struct(left, right, None, None)?
        }
        _ => left.clone(),
    })
}

fn merge_list_struct_helper<T: OffsetSizeTrait>(
    left: &dyn Array,
    right: &dyn Array,
    null_left: bool,
    null_right: bool,
) -> Result<ArrayRef> {
    let left_list: &GenericListArray<T> = left.as_list();
    let right_list: &GenericListArray<T> = right.as_list();
    let ((DataType::List(left_item), DataType::List(right_item))
    | (DataType::LargeList(left_item), DataType::LargeList(right_item))) =
        (left.data_type(), right.data_type())
    else {
        unreachable!()
    };
    let (merged_items, list, nulls, items_nullable) = if null_left || null_right {
        // One side all-null (a placeholder for columns this fragment lacks) has
        // no values, so stand in nulls shaped like the other side's.
        let not_null = if null_left { right_list } else { left_list };
        let values_len = not_null.values().len();
        let values = |list: &GenericListArray<T>, is_null: bool| {
            if is_null {
                new_null_array(list.values().data_type(), values_len)
            } else {
                list.values().clone()
            }
        };
        let merged = merge_list_values(
            &values(left_list, null_left),
            &values(right_list, null_right),
        )?;
        (
            merged,
            not_null,
            not_null.nulls().cloned(),
            left_item.is_nullable() || right_item.is_nullable(),
        )
    } else {
        if left_list.offsets().inner() != right_list.offsets().inner() {
            panic!("Attempt to merge list struct arrays which do not have same offsets");
        }
        // The lists share offsets, and an entry is valid if either side has it,
        // as for struct rows. Each side's items under its own null entries are
        // hidden first, so the other side's validity cannot expose them.
        let left_items = hide_items_revealed_by(left_list, right_list.nulls());
        let right_items = hide_items_revealed_by(right_list, left_list.nulls());
        let merged = merge_list_values(&left_items, &right_items)?;
        (
            merged,
            left_list,
            merge_struct_validity(left_list.nulls(), right_list.nulls()),
            left_item.is_nullable() || right_item.is_nullable(),
        )
    };
    let items_field = Arc::new(Field::new(
        left_item.name(),
        merged_items.data_type().clone(),
        items_nullable,
    ));
    Ok(Arc::new(GenericListArray::<T>::new(
        items_field,
        list.offsets().clone(),
        merged_items,
        nulls,
    )))
}

/// Merge two lists whose items hold structs. Only succeeds if both lists have
/// the same offsets or one of them is all-null; panics otherwise.
///
/// `left_parent` and `right_parent`, when given, null each side's entries under
/// its null parent rows before the merge.
fn merge_list_struct(
    left: &ArrayRef,
    right: &ArrayRef,
    left_parent: Option<&NullBuffer>,
    right_parent: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    // The parents are applied before the all-null side is picked, so a side
    // whose rows all sit under null parents counts as the placeholder and the
    // other side's offsets are kept.
    let left = adjust_child_validity(left, left_parent);
    let right = adjust_child_validity(right, right_parent);
    let null_left = left.null_count() == left.len();
    let null_right = !null_left && right.null_count() == right.len();
    let (left, right) = (left.as_ref(), right.as_ref());
    match (left.data_type(), right.data_type()) {
        (DataType::List(_), DataType::List(_)) => {
            merge_list_struct_helper::<i32>(left, right, null_left, null_right)
        }
        (DataType::LargeList(_), DataType::LargeList(_)) => {
            merge_list_struct_helper::<i64>(left, right, null_left, null_right)
        }
        _ => unreachable!(),
    }
}

/// Helper function to merge validity buffers from two struct arrays.
///
/// A row is valid if it is valid in either input.
/// An absent validity buffer means all rows are valid, an all-null buffer acts as the identity for this merge.
fn merge_struct_validity(
    left_validity: Option<&arrow_buffer::NullBuffer>,
    right_validity: Option<&arrow_buffer::NullBuffer>,
) -> Option<arrow_buffer::NullBuffer> {
    match (left_validity, right_validity) {
        // Fast paths: no computation needed
        (None, _) | (_, None) => None,
        (Some(left), Some(right)) => {
            if left.null_count() == 0 || right.null_count() == 0 {
                return None;
            }
            if left.null_count() == left.len() {
                return Some(right.clone());
            }
            if right.null_count() == right.len() {
                return Some(left.clone());
            }

            let left_buffer = left.inner();
            let right_buffer = right.inner();

            // Perform bitwise OR directly on BooleanBuffers
            // This preserves the correct semantics: 1 = valid, 0 = null
            let merged_buffer = left_buffer | right_buffer;

            Some(arrow_buffer::NullBuffer::from(merged_buffer))
        }
    }
}

fn merge_list_child_values(
    child_field: &Field,
    left_values: ArrayRef,
    right_values: ArrayRef,
) -> ArrayRef {
    match child_field.data_type() {
        DataType::Struct(child_fields) => Arc::new(merge_with_schema(
            left_values.as_struct(),
            right_values.as_struct(),
            child_fields,
        )) as ArrayRef,
        DataType::List(grandchild) => {
            let left_list = left_values
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("left list values should be ListArray");
            let right_list = right_values
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("right list values should be ListArray");
            let merged_values = merge_list_child_values(
                grandchild.as_ref(),
                left_list.values().clone(),
                right_list.values().clone(),
            );
            let merged_validity = merge_struct_validity(left_list.nulls(), right_list.nulls());
            Arc::new(ListArray::new(
                grandchild.clone(),
                left_list.offsets().clone(),
                merged_values,
                merged_validity,
            )) as ArrayRef
        }
        DataType::LargeList(grandchild) => {
            let left_list = left_values
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("left list values should be LargeListArray");
            let right_list = right_values
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("right list values should be LargeListArray");
            let merged_values = merge_list_child_values(
                grandchild.as_ref(),
                left_list.values().clone(),
                right_list.values().clone(),
            );
            let merged_validity = merge_struct_validity(left_list.nulls(), right_list.nulls());
            Arc::new(LargeListArray::new(
                grandchild.clone(),
                left_list.offsets().clone(),
                merged_values,
                merged_validity,
            )) as ArrayRef
        }
        DataType::FixedSizeList(grandchild, list_size) => {
            let left_list = left_values
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("left list values should be FixedSizeListArray");
            let right_list = right_values
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("right list values should be FixedSizeListArray");
            let merged_values = merge_list_child_values(
                grandchild.as_ref(),
                left_list.values().clone(),
                right_list.values().clone(),
            );
            let merged_validity = merge_struct_validity(left_list.nulls(), right_list.nulls());
            Arc::new(FixedSizeListArray::new(
                grandchild.clone(),
                *list_size,
                merged_values,
                merged_validity,
            )) as ArrayRef
        }
        _ => left_values.clone(),
    }
}

// Helper function to adjust child array validity based on parent struct validity
// When parent struct is null, propagates null to child array
// Optimized with fast paths and SIMD operations
fn adjust_child_validity(
    child: &ArrayRef,
    parent_validity: Option<&arrow_buffer::NullBuffer>,
) -> ArrayRef {
    // Fast path: no parent validity means no adjustment needed
    let parent_validity = match parent_validity {
        None => return child.clone(),
        Some(p) if p.null_count() == 0 => return child.clone(), // No nulls to propagate
        Some(p) => p,
    };

    // Fast path: DataType::Null arrays are always entirely null by definition and cannot
    // carry an explicit null bitmap (Arrow rejects it). No adjustment is needed.
    if child.data_type() == &DataType::Null {
        return child.clone();
    }

    let child_validity = child.nulls();

    // Compute the new validity: child_validity AND parent_validity
    let new_validity = match child_validity {
        None => {
            // Fast path: child has no existing validity, just use parent's
            parent_validity.clone()
        }
        Some(child_nulls) => {
            let child_buffer = child_nulls.inner();
            let parent_buffer = parent_validity.inner();

            // Perform bitwise AND directly on BooleanBuffers
            // This preserves the correct semantics: 1 = valid, 0 = null
            let merged_buffer = child_buffer & parent_buffer;

            arrow_buffer::NullBuffer::from(merged_buffer)
        }
    };

    // ArrayData keeps the child's offset for its value buffers. Its null bitmap
    // must cover that offset too, even though the merged validity describes only
    // the visible rows. Copy logical bits into an offset-aligned bitmap when needed.
    let null_bit_buffer = if child.offset() == 0 && new_validity.inner().offset() == 0 {
        new_validity.into_inner().into_inner()
    } else {
        let mut builder =
            arrow_buffer::builder::BooleanBufferBuilder::new(child.offset() + child.len());
        builder.append_n(child.offset(), true);
        builder.append_buffer(new_validity.inner());
        builder.build().into_inner()
    };

    // Create new array with adjusted validity
    arrow_array::make_array(
        arrow_data::ArrayData::try_new(
            child.data_type().clone(),
            child.len(),
            Some(null_bit_buffer),
            child.offset(),
            child.to_data().buffers().to_vec(),
            child.to_data().child_data().to_vec(),
        )
        .unwrap(),
    )
}

/// Fails if `field`, a non-nullable child of one side's struct, sits under a
/// row that is null on that side but valid in `merged`. The merged row is valid
/// because the other side has it, and this side has no value to show there.
fn check_hideable(
    field: &Field,
    parent: Option<&NullBuffer>,
    merged: Option<&NullBuffer>,
) -> Result<()> {
    let Some(parent) = parent.filter(|_| !field.is_nullable()) else {
        return Ok(());
    };
    let revealed = match merged {
        Some(merged) => merged.inner() & &!parent.inner(),
        None => !parent.inner(),
    };
    match revealed.set_indices().next() {
        None => Ok(()),
        Some(row) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot merge non-nullable field '{}': its parent is null on one side but valid \
             on the other at row {row}, where the field has no value",
            field.name()
        ))),
    }
}

/// `column`, a child of one side's struct, with that struct's null rows pushed
/// into it. The merged struct is valid where either side is, so the column
/// would otherwise show, at a row only the other side has, what its own null
/// parent hides.
fn hide_under_parent(
    field: &Field,
    column: &ArrayRef,
    parent: Option<&NullBuffer>,
    merged: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    check_hideable(field, parent, merged)?;
    Ok(adjust_child_validity(column, parent))
}

fn merge(left_struct_array: &StructArray, right_struct_array: &StructArray) -> Result<StructArray> {
    let mut fields: Vec<Field> = vec![];
    let mut columns: Vec<ArrayRef> = vec![];
    let right_fields = right_struct_array.fields();
    let right_columns = right_struct_array.columns();

    // Get the validity buffers from both structs
    let left_validity = left_struct_array.nulls();
    let right_validity = right_struct_array.nulls();

    // Compute merged validity
    let merged_validity = merge_struct_validity(left_validity, right_validity);
    let merged = merged_validity.as_ref();

    // iterate through the fields on the left hand side
    for (left_field, left_column) in left_struct_array
        .fields()
        .iter()
        .zip(left_struct_array.columns().iter())
    {
        match right_fields
            .iter()
            .position(|f| f.name() == left_field.name())
        {
            // if the field exists on the right hand side, merge them recursively if appropriate
            Some(right_index) => {
                let right_field = right_fields.get(right_index).unwrap();
                let right_column = right_columns.get(right_index).unwrap();
                // if both fields are struct, merge them recursively
                match (left_field.data_type(), right_field.data_type()) {
                    (DataType::Struct(_), DataType::Struct(_)) => {
                        // Both sides feed this column, so a row one side's parent
                        // hides can still take its value from the other side, and
                        // the field's nullability is checked on the merged result.
                        let left_sub_array = adjust_child_validity(left_column, left_validity);
                        let right_sub_array = adjust_child_validity(right_column, right_validity);
                        let merged_sub_array =
                            merge(left_sub_array.as_struct(), right_sub_array.as_struct())?;
                        fields.push(Field::new(
                            left_field.name(),
                            merged_sub_array.data_type().clone(),
                            left_field.is_nullable(),
                        ));
                        columns.push(Arc::new(merged_sub_array) as ArrayRef);
                    }
                    (DataType::List(left_list), DataType::List(right_list))
                    | (DataType::LargeList(left_list), DataType::LargeList(right_list))
                        if holds_struct(left_list.data_type())
                            && holds_struct(right_list.data_type()) =>
                    {
                        // Identical inner types: nothing to merge, keep the left column.
                        if left_list.data_type() == right_list.data_type() {
                            fields.push(left_field.as_ref().clone());
                            columns.push(hide_under_parent(
                                left_field,
                                left_column,
                                left_validity,
                                merged,
                            )?);
                        } else {
                            // The struct fields differ, so merge them structurally. merge_list_struct
                            // only succeeds when both lists share offsets or one side is all-null;
                            // it panics otherwise.
                            let merged_sub_array = merge_list_struct(
                                left_column,
                                right_column,
                                left_validity,
                                right_validity,
                            )?;

                            fields.push(Field::new(
                                left_field.name(),
                                merged_sub_array.data_type().clone(),
                                left_field.is_nullable(),
                            ));
                            columns.push(merged_sub_array);
                        }
                    }
                    // otherwise, just use the field on the left hand side
                    _ => {
                        // TODO handle list-of-struct and other types
                        fields.push(left_field.as_ref().clone());
                        columns.push(hide_under_parent(
                            left_field,
                            left_column,
                            left_validity,
                            merged,
                        )?);
                    }
                }
            }
            None => {
                fields.push(left_field.as_ref().clone());
                columns.push(hide_under_parent(
                    left_field,
                    left_column,
                    left_validity,
                    merged,
                )?);
            }
        }
    }

    // now iterate through the fields on the right hand side
    for (field, column) in right_fields.iter().zip(right_columns.iter()) {
        // add new columns on the right
        if !left_struct_array
            .fields()
            .iter()
            .any(|f| f.name() == field.name())
        {
            fields.push(field.as_ref().clone());
            columns.push(hide_under_parent(field, column, right_validity, merged)?);
        }
    }

    StructArray::try_new(Fields::from(fields), columns, merged_validity)
}

fn merge_with_schema(
    left_struct_array: &StructArray,
    right_struct_array: &StructArray,
    fields: &Fields,
) -> StructArray {
    // Helper function that returns true if both types are struct or both are non-struct
    fn same_type_kind(left: &DataType, right: &DataType) -> bool {
        match (left, right) {
            (DataType::Struct(_), DataType::Struct(_)) => true,
            (DataType::Struct(_), _) => false,
            (_, DataType::Struct(_)) => false,
            _ => true,
        }
    }

    let mut output_fields: Vec<Field> = Vec::with_capacity(fields.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());

    let left_fields = left_struct_array.fields();
    let left_columns = left_struct_array.columns();
    let right_fields = right_struct_array.fields();
    let right_columns = right_struct_array.columns();

    // Get the validity buffers from both structs
    let left_validity = left_struct_array.nulls();
    let right_validity = right_struct_array.nulls();

    // Compute merged validity
    let merged_validity = merge_struct_validity(left_validity, right_validity);

    for field in fields {
        let left_match_idx = left_fields.iter().position(|f| {
            f.name() == field.name() && same_type_kind(f.data_type(), field.data_type())
        });
        let right_match_idx = right_fields.iter().position(|f| {
            f.name() == field.name() && same_type_kind(f.data_type(), field.data_type())
        });

        match (left_match_idx, right_match_idx) {
            (None, Some(right_idx)) => {
                output_fields.push(right_fields[right_idx].as_ref().clone());
                // Adjust validity if the right struct was null
                let adjusted_column =
                    adjust_child_validity(&right_columns[right_idx], right_validity);
                columns.push(adjusted_column);
            }
            (Some(left_idx), None) => {
                output_fields.push(left_fields[left_idx].as_ref().clone());
                // Adjust validity if the left struct was null
                let adjusted_column = adjust_child_validity(&left_columns[left_idx], left_validity);
                columns.push(adjusted_column);
            }
            (Some(left_idx), Some(right_idx)) => {
                match field.data_type() {
                    DataType::Struct(child_fields) => {
                        let left_sub_array = left_columns[left_idx].as_struct();
                        let right_sub_array = right_columns[right_idx].as_struct();
                        let merged_sub_array =
                            merge_with_schema(left_sub_array, right_sub_array, child_fields);
                        output_fields.push(Field::new(
                            field.name(),
                            merged_sub_array.data_type().clone(),
                            field.is_nullable(),
                        ));
                        columns.push(Arc::new(merged_sub_array) as ArrayRef);
                    }
                    DataType::List(child_field) => {
                        let left_list = left_columns[left_idx]
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                        let right_list = right_columns[right_idx]
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap();
                        let merged_values = merge_list_child_values(
                            child_field.as_ref(),
                            left_list.trimmed_values(),
                            right_list.trimmed_values(),
                        );
                        let merged_validity =
                            merge_struct_validity(left_list.nulls(), right_list.nulls());
                        // `trimmed_values` starts at the first used value, so offsets
                        // must be shifted to match or `ListArray::new` panics when the
                        // input list was sliced (e.g. from a filtered batch).
                        let merged_list = ListArray::new(
                            child_field.clone(),
                            left_list.trimmed_offsets(),
                            merged_values,
                            merged_validity,
                        );
                        output_fields.push(field.as_ref().clone());
                        columns.push(Arc::new(merged_list) as ArrayRef);
                    }
                    DataType::LargeList(child_field) => {
                        let left_list = left_columns[left_idx]
                            .as_any()
                            .downcast_ref::<LargeListArray>()
                            .unwrap();
                        let right_list = right_columns[right_idx]
                            .as_any()
                            .downcast_ref::<LargeListArray>()
                            .unwrap();
                        let merged_values = merge_list_child_values(
                            child_field.as_ref(),
                            left_list.trimmed_values(),
                            right_list.trimmed_values(),
                        );
                        let merged_validity =
                            merge_struct_validity(left_list.nulls(), right_list.nulls());
                        let merged_list = LargeListArray::new(
                            child_field.clone(),
                            left_list.trimmed_offsets(),
                            merged_values,
                            merged_validity,
                        );
                        output_fields.push(field.as_ref().clone());
                        columns.push(Arc::new(merged_list) as ArrayRef);
                    }
                    DataType::FixedSizeList(child_field, list_size) => {
                        let left_list = left_columns[left_idx]
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .unwrap();
                        let right_list = right_columns[right_idx]
                            .as_any()
                            .downcast_ref::<FixedSizeListArray>()
                            .unwrap();
                        let merged_values = merge_list_child_values(
                            child_field.as_ref(),
                            left_list.values().clone(),
                            right_list.values().clone(),
                        );
                        let merged_validity =
                            merge_struct_validity(left_list.nulls(), right_list.nulls());
                        let merged_list = FixedSizeListArray::new(
                            child_field.clone(),
                            *list_size,
                            merged_values,
                            merged_validity,
                        );
                        output_fields.push(field.as_ref().clone());
                        columns.push(Arc::new(merged_list) as ArrayRef);
                    }
                    _ => {
                        output_fields.push(left_fields[left_idx].as_ref().clone());
                        // For fields that exist in both, use left but adjust validity
                        let adjusted_column =
                            adjust_child_validity(&left_columns[left_idx], left_validity);
                        columns.push(adjusted_column);
                    }
                }
            }
            (None, None) => {
                // The field will not be included in the output
            }
        }
    }

    StructArray::try_new(Fields::from(output_fields), columns, merged_validity).unwrap()
}

fn get_sub_array<'a>(array: &'a ArrayRef, components: &[&str]) -> Option<&'a ArrayRef> {
    if components.is_empty() {
        return Some(array);
    }
    if !matches!(array.data_type(), DataType::Struct(_)) {
        return None;
    }
    let struct_arr = array.as_struct();
    struct_arr
        .column_by_name(components[0])
        .and_then(|arr| get_sub_array(arr, &components[1..]))
}

/// Interleave multiple RecordBatches into a single RecordBatch.
///
/// Behaves like [`arrow_select::interleave::interleave`], but for RecordBatches.
pub fn interleave_batches(
    batches: &[RecordBatch],
    indices: &[(usize, usize)],
) -> Result<RecordBatch> {
    let first_batch = batches.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("Cannot interleave zero RecordBatches".to_string())
    })?;
    let schema = first_batch.schema();
    let num_columns = first_batch.num_columns();
    let mut columns = Vec::with_capacity(num_columns);
    let mut chunks = Vec::with_capacity(batches.len());

    for i in 0..num_columns {
        for batch in batches {
            chunks.push(batch.column(i).as_ref());
        }
        let new_column = interleave(&chunks, indices)?;
        columns.push(new_column);
        chunks.clear();
    }

    RecordBatch::try_new(schema, columns)
}

pub trait BufferExt {
    /// Create an `arrow_buffer::Buffer`` from a `bytes::Bytes` object
    ///
    /// The alignment must be specified (as `bytes_per_value`) since we want to make
    /// sure we can safely reinterpret the buffer.
    ///
    /// If the buffer is properly aligned this will be zero-copy.  If not, a copy
    /// will be made and an owned buffer returned.
    ///
    /// If `bytes_per_value` is not a power of two, then we assume the buffer is
    /// never going to be reinterpreted into another type and we can safely
    /// ignore the alignment.
    ///
    /// Yes, the method name is odd.  It's because there is already a `from_bytes`
    /// which converts from `arrow_buffer::bytes::Bytes` (not `bytes::Bytes`)
    fn from_bytes_bytes(bytes: bytes::Bytes, bytes_per_value: u64) -> Self;

    /// Allocates a new properly aligned arrow buffer and copies `bytes` into it
    ///
    /// `size_bytes` can be larger than `bytes` and, if so, the trailing bytes will
    /// be zeroed out.
    ///
    /// # Panics
    ///
    /// Panics if `size_bytes` is less than `bytes.len()`
    fn copy_bytes_bytes(bytes: bytes::Bytes, size_bytes: usize) -> Self;
}

fn is_pwr_two(n: u64) -> bool {
    n & (n - 1) == 0
}

impl BufferExt for arrow_buffer::Buffer {
    fn from_bytes_bytes(bytes: bytes::Bytes, bytes_per_value: u64) -> Self {
        if is_pwr_two(bytes_per_value) && bytes.as_ptr().align_offset(bytes_per_value as usize) != 0
        {
            // The original buffer is not aligned, cannot zero-copy
            let size_bytes = bytes.len();
            Self::copy_bytes_bytes(bytes, size_bytes)
        } else {
            // The original buffer is aligned, can zero-copy
            // SAFETY: the alignment is correct we can make this conversion
            unsafe {
                Self::from_custom_allocation(
                    NonNull::new(bytes.as_ptr() as _).expect("should be a valid pointer"),
                    bytes.len(),
                    Arc::new(bytes),
                )
            }
        }
    }

    fn copy_bytes_bytes(bytes: bytes::Bytes, size_bytes: usize) -> Self {
        assert!(size_bytes >= bytes.len());
        let mut buf = MutableBuffer::with_capacity(size_bytes);
        let to_fill = size_bytes - bytes.len();
        buf.extend(bytes);
        buf.extend(std::iter::repeat_n(0_u8, to_fill));

        // FIX for issue #4512: Shrink buffer to actual size before converting to immutable
        // This reduces memory overhead from capacity over-allocation
        buf.shrink_to_fit();

        Self::from(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int32Type;
    use arrow_array::{
        BinaryArray, BinaryViewArray, BooleanArray, FixedSizeBinaryArray, Float32Array, Int32Array,
        LargeBinaryArray, ListArray, NullArray, StringArray, StructArray, new_empty_array,
        new_null_array,
    };
    use arrow_buffer::OffsetBuffer;

    #[test]
    fn test_adjust_child_validity_with_sliced_child() {
        let child: ArrayRef = Arc::new(BooleanArray::from_iter((0..1030).map(|index| {
            if index == 1027 {
                None
            } else {
                Some(index % 2 == 1)
            }
        })));
        let child = child.slice(1025, 4);
        assert_eq!(child.offset(), 1025);

        let parent_validity = arrow_buffer::NullBuffer::from(vec![true, false, true, true]);
        let adjusted = adjust_child_validity(&child, Some(&parent_validity));
        assert_eq!(adjusted.offset(), child.offset());
        let adjusted = adjusted.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            adjusted.iter().collect::<Vec<_>>(),
            vec![Some(true), None, None, Some(false)]
        );
    }

    #[test]
    fn test_adjust_child_validity_with_sliced_parent_bitmap() {
        let child: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let parent_validity: arrow_buffer::NullBuffer =
            (0..1030).map(|index| index != 1026).collect();
        let parent_validity = parent_validity.slice(1025, 4);
        assert_eq!(parent_validity.inner().offset(), 1025);

        let adjusted = adjust_child_validity(&child, Some(&parent_validity));
        let adjusted = adjusted.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            adjusted.iter().collect::<Vec<_>>(),
            vec![Some(1), None, Some(3), Some(4)]
        );
    }

    #[test]
    fn test_convert_to_floating_point_preserves_inner_nulls() {
        // A FixedSizeList<Int8> with a null inner element must convert to a
        // FixedSizeList<Float32> with the null kept in place. Dropping it would
        // shorten the values array and shift every later element (and, when the
        // remaining count is not a multiple of the list size, panic).
        let values = Int8Array::from(vec![Some(1), None, Some(3), Some(4)]);
        let fsl = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int8, true)),
            2,
            Arc::new(values),
            None,
        );

        let converted = fsl.convert_to_floating_point().unwrap();

        assert_eq!(converted.len(), 2);
        let conv_values = converted
            .values()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(conv_values.len(), 4);
        assert_eq!(conv_values.value(0), 1.0);
        assert!(conv_values.is_null(1));
        assert_eq!(conv_values.value(2), 3.0);
        assert_eq!(conv_values.value(3), 4.0);
    }

    #[test]
    fn test_convert_to_floating_point_preserves_inner_nulls_f64_arm() {
        // The Float64-producing arms (Int64/UInt8/UInt32) share the same fix as the
        // Float32 arms; cover one representative (UInt8 -> Float64) so both branch
        // families are exercised.
        let values = UInt8Array::from(vec![Some(10u8), None, Some(30), Some(40)]);
        let fsl = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::UInt8, true)),
            2,
            Arc::new(values),
            None,
        );

        let converted = fsl.convert_to_floating_point().unwrap();

        assert_eq!(converted.len(), 2);
        let conv_values = converted
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(conv_values.len(), 4);
        assert_eq!(conv_values.value(0), 10.0);
        assert!(conv_values.is_null(1));
        assert_eq!(conv_values.value(2), 30.0);
        assert_eq!(conv_values.value(3), 40.0);
    }

    #[test]
    fn test_merge_recursive() {
        let a_array = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let e_array = Int32Array::from(vec![Some(4), Some(5), Some(6)]);
        let c_array = Int32Array::from(vec![Some(7), Some(8), Some(9)]);
        let d_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::Struct(vec![Field::new("c", DataType::Int32, true)].into()),
                true,
            ),
        ]);
        let left_batch = RecordBatch::try_new(
            Arc::new(left_schema),
            vec![
                Arc::new(a_array.clone()),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("c", DataType::Int32, true)),
                    Arc::new(c_array.clone()) as ArrayRef,
                )])),
            ],
        )
        .unwrap();

        let right_schema = Schema::new(vec![
            Field::new("e", DataType::Int32, true),
            Field::new(
                "b",
                DataType::Struct(vec![Field::new("d", DataType::Utf8, true)].into()),
                true,
            ),
        ]);
        let right_batch = RecordBatch::try_new(
            Arc::new(right_schema),
            vec![
                Arc::new(e_array.clone()),
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("d", DataType::Utf8, true)),
                    Arc::new(d_array.clone()) as ArrayRef,
                )])) as ArrayRef,
            ],
        )
        .unwrap();

        let merged_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new(
                "b",
                DataType::Struct(
                    vec![
                        Field::new("c", DataType::Int32, true),
                        Field::new("d", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new("e", DataType::Int32, true),
        ]);
        let merged_batch = RecordBatch::try_new(
            Arc::new(merged_schema),
            vec![
                Arc::new(a_array) as ArrayRef,
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("c", DataType::Int32, true)),
                        Arc::new(c_array) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("d", DataType::Utf8, true)),
                        Arc::new(d_array) as ArrayRef,
                    ),
                ])) as ArrayRef,
                Arc::new(e_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = left_batch.merge(&right_batch).unwrap();
        assert_eq!(result, merged_batch);
    }

    #[test]
    fn test_merge_with_schema() {
        fn test_batch(names: &[&str], types: &[DataType]) -> (Schema, RecordBatch) {
            let fields: Fields = names
                .iter()
                .zip(types)
                .map(|(name, ty)| Field::new(name.to_string(), ty.clone(), false))
                .collect();
            let schema = Schema::new(vec![Field::new(
                "struct",
                DataType::Struct(fields.clone()),
                false,
            )]);
            let children = types.iter().map(new_empty_array).collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(StructArray::new(fields, children, None)) as ArrayRef],
            );
            (schema, batch.unwrap())
        }

        let (_, left_batch) = test_batch(&["a", "b"], &[DataType::Int32, DataType::Int64]);
        let (_, right_batch) = test_batch(&["c", "b"], &[DataType::Int32, DataType::Int64]);
        let (output_schema, _) = test_batch(
            &["b", "a", "c"],
            &[DataType::Int64, DataType::Int32, DataType::Int32],
        );

        // If we use merge_with_schema the schema is respected
        let merged = left_batch
            .merge_with_schema(&right_batch, &output_schema)
            .unwrap();
        assert_eq!(merged.schema().as_ref(), &output_schema);

        // If we use merge we get first-come first-serve based on the left batch
        let (naive_schema, _) = test_batch(
            &["a", "b", "c"],
            &[DataType::Int32, DataType::Int64, DataType::Int32],
        );
        let merged = left_batch.merge(&right_batch).unwrap();
        assert_eq!(merged.schema().as_ref(), &naive_schema);
    }

    #[test]
    fn test_merge_list_struct() {
        let x_field = Arc::new(Field::new("x", DataType::Int32, true));
        let y_field = Arc::new(Field::new("y", DataType::Int32, true));
        let x_struct_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![x_field.clone()])),
            true,
        ));
        let y_struct_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![y_field.clone()])),
            true,
        ));
        let both_struct_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![x_field.clone(), y_field.clone()])),
            true,
        ));
        let left_schema = Schema::new(vec![Field::new(
            "list_struct",
            DataType::List(x_struct_field.clone()),
            true,
        )]);
        let right_schema = Schema::new(vec![Field::new(
            "list_struct",
            DataType::List(y_struct_field.clone()),
            true,
        )]);
        let both_schema = Schema::new(vec![Field::new(
            "list_struct",
            DataType::List(both_struct_field.clone()),
            true,
        )]);

        let x = Arc::new(Int32Array::from(vec![1]));
        let y = Arc::new(Int32Array::from(vec![2]));
        let x_struct = Arc::new(StructArray::new(
            Fields::from(vec![x_field.clone()]),
            vec![x.clone()],
            None,
        ));
        let y_struct = Arc::new(StructArray::new(
            Fields::from(vec![y_field.clone()]),
            vec![y.clone()],
            None,
        ));
        let both_struct = Arc::new(StructArray::new(
            Fields::from(vec![x_field.clone(), y_field.clone()]),
            vec![x.clone(), y],
            None,
        ));
        let both_null_struct = Arc::new(StructArray::new(
            Fields::from(vec![x_field, y_field]),
            vec![x, Arc::new(new_null_array(&DataType::Int32, 1))],
            None,
        ));
        let offsets = OffsetBuffer::from_lengths([1]);
        let x_s_list = ListArray::new(x_struct_field, offsets.clone(), x_struct, None);
        let y_s_list = ListArray::new(y_struct_field, offsets.clone(), y_struct, None);
        let both_list = ListArray::new(
            both_struct_field.clone(),
            offsets.clone(),
            both_struct,
            None,
        );
        let both_null_list = ListArray::new(both_struct_field, offsets, both_null_struct, None);
        let x_batch =
            RecordBatch::try_new(Arc::new(left_schema), vec![Arc::new(x_s_list)]).unwrap();
        let y_batch = RecordBatch::try_new(
            Arc::new(right_schema.clone()),
            vec![Arc::new(y_s_list.clone())],
        )
        .unwrap();
        let merged = x_batch.merge(&y_batch).unwrap();
        let expected =
            RecordBatch::try_new(Arc::new(both_schema.clone()), vec![Arc::new(both_list)]).unwrap();
        assert_eq!(merged, expected);

        let y_null_list = new_null_array(y_s_list.data_type(), 1);
        let y_null_batch =
            RecordBatch::try_new(Arc::new(right_schema), vec![Arc::new(y_null_list.clone())])
                .unwrap();
        let expected =
            RecordBatch::try_new(Arc::new(both_schema), vec![Arc::new(both_null_list)]).unwrap();
        let merged = x_batch.merge(&y_null_batch).unwrap();
        assert_eq!(merged, expected);
    }

    #[test]
    fn test_merge_all_null_list_adds_nested_child() {
        // An all-null placeholder (what a fragment reader merges in for children
        // missing from its files) adds `b` to a struct below one or two lists.
        let strukt = |cols: Vec<(&str, ArrayRef)>| -> ArrayRef {
            let fields = cols
                .iter()
                .map(|(name, col)| Field::new(*name, col.data_type().clone(), true))
                .collect::<Vec<_>>();
            Arc::new(StructArray::new(
                fields.into(),
                cols.into_iter().map(|(_, col)| col).collect(),
                None,
            ))
        };
        // Two rows: `[values]` and null.
        let list = |values: ArrayRef, large: bool| -> ArrayRef {
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            let lengths = [values.len(), 0];
            let nulls = Some(arrow_buffer::NullBuffer::from(vec![true, false]));
            if large {
                Arc::new(LargeListArray::new(
                    field,
                    OffsetBuffer::from_lengths(lengths),
                    values,
                    nulls,
                ))
            } else {
                Arc::new(ListArray::new(
                    field,
                    OffsetBuffer::from_lengths(lengths),
                    values,
                    nulls,
                ))
            }
        };
        let a = || Arc::new(Int32Array::from(vec![7])) as ArrayRef;
        let b = || new_null_array(&DataType::Utf8, 1);
        for large in [false, true] {
            // list<struct<t, in: struct<..>>>, built with or without `t`.
            let under_list = |inner: ArrayRef, with_t: bool| {
                let mut cols = vec![("in", inner)];
                if with_t {
                    cols.insert(0, ("t", Arc::new(Int32Array::from(vec![1])) as ArrayRef));
                }
                list(strukt(cols), large)
            };
            // list<list<struct<..>>>
            let under_two_lists = |inner: ArrayRef, _: bool| {
                let inner_list: ArrayRef = if large {
                    let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
                    Arc::new(LargeListArray::new(
                        field,
                        OffsetBuffer::from_lengths([1]),
                        inner,
                        None,
                    ))
                } else {
                    let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
                    Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::from_lengths([1]),
                        inner,
                        None,
                    ))
                };
                list(inner_list, large)
            };
            let shapes: [&dyn Fn(ArrayRef, bool) -> ArrayRef; 2] = [&under_list, &under_two_lists];
            for shape in shapes {
                let data = shape(strukt(vec![("a", a())]), true);
                let expected = shape(strukt(vec![("a", a()), ("b", b())]), true);
                let placeholder =
                    new_null_array(shape(strukt(vec![("b", b())]), false).data_type(), 2);
                let batch = |col: ArrayRef| {
                    RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new(
                            "c",
                            col.data_type().clone(),
                            true,
                        )])),
                        vec![col],
                    )
                    .unwrap()
                };
                let expected = batch(expected);
                // Either side may be the placeholder; field order follows the left.
                for merged in [
                    batch(data.clone())
                        .merge(&batch(placeholder.clone()))
                        .unwrap(),
                    batch(placeholder.clone())
                        .merge(&batch(data.clone()))
                        .unwrap(),
                ] {
                    let merged = merged.project_by_schema(expected.schema_ref()).unwrap();
                    assert_eq!(merged, expected, "large={large}");
                }
            }
        }
    }

    #[test]
    fn test_merge_list_struct_identical_schema() {
        // Merging two batches whose `List<Struct>` columns have identical types
        // should yield a single column equal to the input (there is nothing to
        // merge), not a struct with the field pushed twice.
        let x_field = Arc::new(Field::new("x", DataType::Int32, true));
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![x_field.clone()])),
            true,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "list_struct",
            DataType::List(item_field.clone()),
            true,
        )]));

        let build_list = |values: Vec<i32>| {
            let len = values.len();
            let item_struct = Arc::new(StructArray::new(
                Fields::from(vec![x_field.clone()]),
                vec![Arc::new(Int32Array::from(values))],
                None,
            ));
            ListArray::new(
                item_field.clone(),
                OffsetBuffer::from_lengths([len]),
                item_struct,
                None,
            )
        };

        // Distinct values so the equality assertion proves the left column is kept,
        // not that some column with the same values happens to be present.
        let left =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(build_list(vec![1, 2]))]).unwrap();
        let right = RecordBatch::try_new(schema, vec![Arc::new(build_list(vec![3, 4]))]).unwrap();

        let merged = left.merge(&right).unwrap();

        // Exactly one column, equal to the left input: the field must not be duplicated.
        assert_eq!(merged.num_columns(), 1);
        assert_eq!(merged, left);
    }

    #[test]
    fn test_merge_nested_list_struct_identical_schema() {
        // A `List<Struct>` nested inside a struct reaches the identical-type
        // short-circuit through the recursive `merge` call. The recursion must
        // keep the field exactly once, equal to the left input.
        let x_field = Arc::new(Field::new("x", DataType::Int32, true));
        let item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![x_field.clone()])),
            true,
        ));
        let companies_field = Arc::new(Field::new(
            "companies",
            DataType::List(item_field.clone()),
            true,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(Fields::from(vec![companies_field.clone()])),
            true,
        )]));

        let build_outer = |x: i32| {
            // One list row: [{x}].
            let item_struct = Arc::new(StructArray::new(
                Fields::from(vec![x_field.clone()]),
                vec![Arc::new(Int32Array::from(vec![x]))],
                None,
            ));
            let companies = Arc::new(ListArray::new(
                item_field.clone(),
                OffsetBuffer::from_lengths([1]),
                item_struct,
                None,
            ));
            StructArray::new(
                Fields::from(vec![companies_field.clone()]),
                vec![companies],
                None,
            )
        };

        // Distinct values so equality proves the left column is kept.
        let left = RecordBatch::try_new(schema.clone(), vec![Arc::new(build_outer(10))]).unwrap();
        let right = RecordBatch::try_new(schema, vec![Arc::new(build_outer(20))]).unwrap();

        let merged = left.merge(&right).unwrap();

        // The recursive identical-type merge keeps exactly one `companies` field,
        // with no double-push inside the nested struct.
        assert_eq!(merged.column(0).as_struct().num_columns(), 1);
        assert_eq!(merged, left);
    }

    /// A list of `items`, one item per entry.
    fn one_item_per_entry(large: bool, items: ArrayRef) -> ArrayRef {
        let item_field = Arc::new(Field::new("item", items.data_type().clone(), true));
        let lengths = vec![1; items.len()];
        if large {
            Arc::new(LargeListArray::new(
                item_field,
                OffsetBuffer::<i64>::from_lengths(lengths),
                items,
                None,
            ))
        } else {
            Arc::new(ListArray::new(
                item_field,
                OffsetBuffer::<i32>::from_lengths(lengths),
                items,
                None,
            ))
        }
    }

    /// A batch with the single column `s`, valid per `parent_validity`.
    fn batch_of_parent(parent: StructArray) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                parent.data_type().clone(),
                true,
            )])),
            vec![Arc::new(parent)],
        )
        .unwrap()
    }

    /// `{s: struct<l: list<struct<{child}: int32>>>}`, one row per value, each
    /// list holding one item, and `s` valid per `parent_validity`.
    fn list_struct_under_parent(
        large: bool,
        child: &str,
        parent_validity: &[bool],
        values: &[i32],
    ) -> RecordBatch {
        let items = Arc::new(StructArray::new(
            vec![Field::new(child, DataType::Int32, true)].into(),
            vec![Arc::new(Int32Array::from(values.to_vec()))],
            None,
        ));
        let list = one_item_per_entry(large, items);
        batch_of_parent(StructArray::new(
            vec![Field::new("l", list.data_type().clone(), true)].into(),
            vec![list],
            Some(parent_validity.to_vec().into()),
        ))
    }

    /// The child `name` of the single item in row `row` of the merged `s.l`.
    fn merged_item_child(merged: &RecordBatch, large: bool, row: usize, name: &str) -> ArrayRef {
        let list = merged.column(0).as_struct().column(0);
        let item = if large {
            list.as_list::<i64>().value(row)
        } else {
            list.as_list::<i32>().value(row)
        };
        item.as_struct().column_by_name(name).unwrap().clone()
    }

    #[test]
    fn test_merge_list_struct_identical_schema_hides_a_null_parent_row() {
        for large in [false, true] {
            let left = list_struct_under_parent(large, "x", &[false, true], &[11, 12]);
            let right = list_struct_under_parent(large, "x", &[true, true], &[21, 22]);
            let merged = left.merge(&right).unwrap();
            let parent = merged.column(0).as_struct();
            assert!(parent.is_valid(0), "large={large}");
            assert!(
                parent.column(0).is_null(0),
                "large={large}: the list under the null left parent became visible"
            );
            assert!(parent.column(0).is_valid(1), "large={large}");
        }
    }

    #[test]
    fn test_merge_list_struct_hides_the_items_of_a_null_parent_row() {
        for large in [false, true] {
            // Null parent on the left: row 0 keeps the right item, not x = 11.
            let left = list_struct_under_parent(large, "x", &[false, true], &[11, 12]);
            let right = list_struct_under_parent(large, "y", &[true, true], &[21, 22]);
            let merged = left.merge(&right).unwrap();
            assert!(merged.column(0).as_struct().column(0).is_valid(0));
            assert!(
                merged_item_child(&merged, large, 0, "x").is_null(0),
                "large={large}: x under the null left parent became visible"
            );
            let y = merged_item_child(&merged, large, 0, "y");
            assert_eq!(y.as_primitive::<Int32Type>().value(0), 21, "large={large}");

            // Null parent on the right: row 0 keeps x = 11, not y = 21.
            let left = list_struct_under_parent(large, "x", &[true, true], &[11, 12]);
            let right = list_struct_under_parent(large, "y", &[false, true], &[21, 22]);
            let merged = left.merge(&right).unwrap();
            let x = merged_item_child(&merged, large, 0, "x");
            assert_eq!(x.as_primitive::<Int32Type>().value(0), 11, "large={large}");
            assert!(
                merged_item_child(&merged, large, 0, "y").is_null(0),
                "large={large}: y under the null right parent became visible"
            );

            // Row 1 is valid on both sides and keeps both items.
            let x = merged_item_child(&merged, large, 1, "x");
            let y = merged_item_child(&merged, large, 1, "y");
            assert_eq!(x.as_primitive::<Int32Type>().value(0), 12, "large={large}");
            assert_eq!(y.as_primitive::<Int32Type>().value(0), 22, "large={large}");
        }
    }

    #[test]
    fn test_merge_list_struct_follows_a_sliced_parent() {
        for large in [false, true] {
            // After slicing, row 0 is the null parent row and row 1 is valid.
            let left = list_struct_under_parent(large, "x", &[true, false, true], &[10, 11, 12]);
            let left = left.slice(1, 2);

            let same = list_struct_under_parent(large, "x", &[true, true, true], &[20, 21, 22]);
            let merged = left.merge(&same.slice(1, 2)).unwrap();
            let list = merged.column(0).as_struct().column(0);
            assert!(list.is_null(0), "large={large}");
            assert!(list.is_valid(1), "large={large}");

            let other = list_struct_under_parent(large, "y", &[true, true, true], &[20, 21, 22]);
            let merged = left.merge(&other.slice(1, 2)).unwrap();
            assert!(
                merged_item_child(&merged, large, 0, "x").is_null(0),
                "large={large}"
            );
            let x = merged_item_child(&merged, large, 1, "x");
            assert_eq!(x.as_primitive::<Int32Type>().value(0), 12, "large={large}");
        }
    }

    /// A non-nullable `l` or item child cannot be nulled under a parent row that
    /// only the other side has, so the merge fails there rather than show the
    /// hidden value. Where both sides agree on the parent, nothing is revealed
    /// and the same fields merge.
    #[test]
    fn test_merge_list_struct_with_non_nullable_fields_under_a_null_parent() {
        #[derive(Clone, Copy)]
        struct Shape {
            child: &'static str,
            l_nullable: bool,
            item_nullable: bool,
            child_nullable: bool,
            null_first_entry: bool,
            null_first_item: bool,
        }
        let shape = |child| Shape {
            child,
            l_nullable: true,
            item_nullable: true,
            child_nullable: true,
            null_first_entry: false,
            null_first_item: false,
        };
        let batch = |shape: Shape, parent_validity: [bool; 2]| {
            let items = Arc::new(StructArray::new(
                vec![Field::new(
                    shape.child,
                    DataType::Int32,
                    shape.child_nullable,
                )]
                .into(),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
                shape.null_first_item.then(|| vec![false, true].into()),
            ));
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new(
                    "item",
                    items.data_type().clone(),
                    shape.item_nullable,
                )),
                OffsetBuffer::<i32>::from_lengths([1, 1]),
                items,
                shape.null_first_entry.then(|| vec![false, true].into()),
            ));
            batch_of_parent(StructArray::new(
                vec![Field::new("l", list.data_type().clone(), shape.l_nullable)].into(),
                vec![list],
                Some(parent_validity.to_vec().into()),
            ))
        };
        let null_first = [false, true];
        let all_valid = [true, true];
        let cases = [
            // Non-nullable `l`, identical item types.
            (
                Shape {
                    l_nullable: false,
                    ..shape("x")
                },
                Shape {
                    l_nullable: false,
                    ..shape("x")
                },
                Some("Cannot merge non-nullable field 'l'"),
            ),
            // Non-nullable `l` on the left, a null entry on the right: neither
            // side has an entry at the merged row.
            (
                Shape {
                    l_nullable: false,
                    ..shape("x")
                },
                Shape {
                    null_first_entry: true,
                    ..shape("y")
                },
                Some("non-nullable StructArray field \"l\""),
            ),
            // A non-nullable child of the item struct.
            (
                Shape {
                    child_nullable: false,
                    ..shape("x")
                },
                shape("y"),
                Some("Cannot merge non-nullable field 'x'"),
            ),
            // A non-nullable item can be hidden where the right item is null.
            (
                Shape {
                    item_nullable: false,
                    ..shape("x")
                },
                Shape {
                    null_first_item: true,
                    ..shape("y")
                },
                None,
            ),
        ];
        for (case, (left, right, rejected)) in cases.into_iter().enumerate() {
            let merged = batch(left, null_first).merge(&batch(right, all_valid));
            match rejected {
                Some(expected) => {
                    let message = merged.unwrap_err().to_string();
                    assert!(message.contains(expected), "case {case}: {message}");
                }
                None => assert!(merged.is_ok(), "case {case}: {merged:?}"),
            }
            let agreed = batch(left, null_first).merge(&batch(right, null_first));
            assert!(
                agreed.is_ok(),
                "case {case} with agreeing parents: {agreed:?}"
            );
        }
    }

    /// The fragment reader merges a data batch with an all-null placeholder for
    /// a nested child the file lacks. Only the placeholder's top-level field is
    /// nullable, so a non-nullable field below it must still merge, in either
    /// order, keeping the data and filling the new child with nulls.
    #[test]
    fn test_merge_with_a_placeholder_under_a_non_nullable_field() {
        let inner =
            |child: &str| -> Fields { vec![Field::new(child, DataType::Int32, true)].into() };
        for wrap in ["struct", "list", "large_list"] {
            let wrapped = |fields: Fields| match wrap {
                "struct" => DataType::Struct(fields),
                "list" => {
                    DataType::List(Arc::new(Field::new("item", DataType::Struct(fields), true)))
                }
                _ => DataType::LargeList(Arc::new(Field::new(
                    "item",
                    DataType::Struct(fields),
                    true,
                ))),
            };
            let outer = |child: &str| {
                DataType::Struct(vec![Field::new("inner", wrapped(inner(child)), false)].into())
            };
            let items: ArrayRef = Arc::new(StructArray::new(
                inner("a"),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
                None,
            ));
            let data_inner = match wrap {
                "struct" => items,
                "list" => one_item_per_entry(false, items),
                _ => one_item_per_entry(true, items),
            };
            let data = batch_of_parent(StructArray::new(
                vec![Field::new("inner", data_inner.data_type().clone(), false)].into(),
                vec![data_inner],
                None,
            ));
            let placeholder = batch_of_parent(new_null_array(&outer("b"), 2).as_struct().clone());

            for (left, right) in [(&data, &placeholder), (&placeholder, &data)] {
                let merged = left.merge(right).unwrap();
                let inner = merged
                    .column(0)
                    .as_struct()
                    .column_by_name("inner")
                    .unwrap();
                let items = match wrap {
                    "struct" => inner.clone(),
                    "list" => inner.as_list::<i32>().values().clone(),
                    _ => inner.as_list::<i64>().values().clone(),
                };
                let items = items.as_struct();
                let a = items.column_by_name("a").unwrap();
                assert_eq!(a.as_primitive::<Int32Type>().values(), &[1, 2], "{wrap}");
                assert_eq!(a.null_count(), 0, "{wrap}");
                assert_eq!(items.column_by_name("b").unwrap().null_count(), 2, "{wrap}");
            }
        }
    }

    /// The null parent row's values stay hidden below a struct nested in the
    /// list item, and below a plain struct field.
    #[test]
    fn test_merge_hides_nested_structs_of_a_null_parent_row() {
        let nested = |child: &str, values: Vec<i32>| {
            let leaf = StructArray::new(
                vec![Field::new(child, DataType::Int32, true)].into(),
                vec![Arc::new(Int32Array::from(values))],
                None,
            );
            StructArray::new(
                vec![Field::new("payload", leaf.data_type().clone(), true)].into(),
                vec![Arc::new(leaf)],
                None,
            )
        };
        let x_of = |payload: &dyn Array, row: usize| {
            let x = payload.as_struct().column_by_name("x").unwrap().clone();
            x.is_null(row)
        };

        for large in [false, true] {
            let in_list = |child: &str, parent: [bool; 2]| {
                let list = one_item_per_entry(large, Arc::new(nested(child, vec![11, 12])));
                batch_of_parent(StructArray::new(
                    vec![Field::new("l", list.data_type().clone(), true)].into(),
                    vec![list],
                    Some(parent.to_vec().into()),
                ))
            };
            let merged = in_list("x", [false, true])
                .merge(&in_list("y", [true, true]))
                .unwrap();
            let list = merged.column(0).as_struct().column(0);
            let item = if large {
                list.as_list::<i64>().value(0)
            } else {
                list.as_list::<i32>().value(0)
            };
            let payload = item.as_struct().column_by_name("payload").unwrap();
            assert!(x_of(payload.as_ref(), 0), "large={large}");
        }

        let in_struct = |child: &str, parent: [bool; 2]| {
            let inner = nested(child, vec![11, 12]);
            batch_of_parent(StructArray::new(
                vec![Field::new("c", inner.data_type().clone(), true)].into(),
                vec![Arc::new(inner)],
                Some(parent.to_vec().into()),
            ))
        };
        let merged = in_struct("x", [false, true])
            .merge(&in_struct("y", [true, true]))
            .unwrap();
        let c = merged.column(0).as_struct().column(0);
        let payload = c.as_struct().column_by_name("payload").unwrap();
        assert!(x_of(payload.as_ref(), 0));
        assert!(!x_of(payload.as_ref(), 1));
    }

    /// A batch whose parent rows are all null merges with an all-null
    /// placeholder whose item struct holds more fields.
    #[test]
    fn test_merge_list_struct_under_null_parents_with_a_placeholder() {
        for large in [false, true] {
            let a = Field::new("a", DataType::Int32, true);
            let inner = StructArray::new(
                vec![a.clone()].into(),
                vec![Arc::new(Int32Array::from(vec![1, 2]))],
                None,
            );
            let items = Arc::new(StructArray::new(
                vec![Field::new("x", inner.data_type().clone(), true)].into(),
                vec![Arc::new(inner)],
                None,
            ));
            let list = one_item_per_entry(large, items);
            let left = batch_of_parent(StructArray::new(
                vec![Field::new("l", list.data_type().clone(), true)].into(),
                vec![list],
                Some(vec![false, false].into()),
            ));

            let wider_x =
                DataType::Struct(vec![a.clone(), Field::new("b", DataType::Int32, true)].into());
            let wider_items = new_null_array(
                &DataType::Struct(vec![Field::new("x", wider_x, true)].into()),
                2,
            );
            let wider_list = one_item_per_entry(large, wider_items);
            let placeholder = new_null_array(
                &DataType::Struct(
                    vec![Field::new("l", wider_list.data_type().clone(), true)].into(),
                ),
                2,
            );
            let right = batch_of_parent(placeholder.as_struct().clone());

            let merged = left.merge(&right).unwrap();
            assert_eq!(merged.column(0).null_count(), 2, "large={large}");
        }
    }

    /// A side whose list entries are all hidden by null parents merges like a
    /// placeholder, so the other side's lists survive even with other offsets.
    #[test]
    fn test_merge_list_struct_hidden_side_keeps_the_other_offsets() {
        for large in [false, true] {
            let list_of = |child: &str, lengths: Vec<usize>, values: Vec<i32>| -> ArrayRef {
                let items: ArrayRef = Arc::new(StructArray::new(
                    vec![Field::new(child, DataType::Int32, true)].into(),
                    vec![Arc::new(Int32Array::from(values))],
                    None,
                ));
                let field = Arc::new(Field::new("item", items.data_type().clone(), true));
                if large {
                    Arc::new(LargeListArray::new(
                        field,
                        OffsetBuffer::<i64>::from_lengths(lengths),
                        items,
                        None,
                    ))
                } else {
                    Arc::new(ListArray::new(
                        field,
                        OffsetBuffer::<i32>::from_lengths(lengths),
                        items,
                        None,
                    ))
                }
            };
            let parent = |list: ArrayRef, validity: Vec<bool>| {
                batch_of_parent(StructArray::new(
                    vec![Field::new("l", list.data_type().clone(), true)].into(),
                    vec![list],
                    Some(validity.into()),
                ))
            };
            let left = parent(list_of("x", vec![1, 1], vec![1, 2]), vec![false, false]);
            let right = parent(list_of("y", vec![2, 0], vec![7, 8]), vec![true, true]);

            let merged = left.merge(&right).unwrap();
            let list = merged.column(0).as_struct().column(0);
            let (first, second) = if large {
                let list = list.as_list::<i64>();
                (list.value(0), list.value(1))
            } else {
                let list = list.as_list::<i32>();
                (list.value(0), list.value(1))
            };
            let first = first.as_struct();
            assert_eq!(
                first
                    .column_by_name("y")
                    .unwrap()
                    .as_primitive::<Int32Type>()
                    .values(),
                &[7, 8],
                "large={large}"
            );
            assert_eq!(
                first.column_by_name("x").unwrap().null_count(),
                2,
                "large={large}"
            );
            assert_eq!(second.len(), 0, "large={large}");
        }
    }

    #[test]
    fn test_byte_width_opt() {
        assert_eq!(DataType::Int32.byte_width_opt(), Some(4));
        assert_eq!(DataType::Int64.byte_width_opt(), Some(8));
        assert_eq!(DataType::Float32.byte_width_opt(), Some(4));
        assert_eq!(DataType::Float64.byte_width_opt(), Some(8));
        assert_eq!(DataType::Utf8.byte_width_opt(), None);
        assert_eq!(DataType::Binary.byte_width_opt(), None);
        assert_eq!(
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))).byte_width_opt(),
            None
        );
        assert_eq!(
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3)
                .byte_width_opt(),
            Some(12)
        );
        assert_eq!(
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 4)
                .byte_width_opt(),
            Some(16)
        );
        assert_eq!(
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Utf8, true)), 5)
                .byte_width_opt(),
            None
        );
    }

    #[test]
    fn test_take_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..20)),
                Arc::new(StringArray::from_iter_values(
                    (0..20).map(|i| format!("str-{}", i)),
                )),
            ],
        )
        .unwrap();
        let taken = batch.take(&(vec![1_u32, 5_u32, 10_u32].into())).unwrap();
        assert_eq!(
            taken,
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 5, 10])),
                    Arc::new(StringArray::from(vec!["str-1", "str-5", "str-10"])),
                ],
            )
            .unwrap()
        )
    }

    #[test]
    fn test_schema_project_by_schema() {
        let metadata = [("key".to_string(), "value".to_string())];
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ])
            .with_metadata(metadata.clone().into()),
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..20)),
                Arc::new(StringArray::from_iter_values(
                    (0..20).map(|i| format!("str-{}", i)),
                )),
            ],
        )
        .unwrap();

        // Empty schema
        let empty_schema = Schema::empty();
        let empty_projected = batch.project_by_schema(&empty_schema).unwrap();
        let expected_schema = empty_schema.with_metadata(metadata.clone().into());
        assert_eq!(
            empty_projected,
            RecordBatch::from(StructArray::new_empty_fields(batch.num_rows(), None))
                .with_schema(Arc::new(expected_schema))
                .unwrap()
        );

        // Re-ordered schema
        let reordered_schema = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
        ]);
        let reordered_projected = batch.project_by_schema(&reordered_schema).unwrap();
        let expected_schema = Arc::new(reordered_schema.with_metadata(metadata.clone().into()));
        assert_eq!(
            reordered_projected,
            RecordBatch::try_new(
                expected_schema,
                vec![
                    Arc::new(StringArray::from_iter_values(
                        (0..20).map(|i| format!("str-{}", i)),
                    )),
                    Arc::new(Int32Array::from_iter_values(0..20)),
                ],
            )
            .unwrap()
        );

        // Sub schema
        let sub_schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let sub_projected = batch.project_by_schema(&sub_schema).unwrap();
        let expected_schema = Arc::new(sub_schema.with_metadata(metadata.into()));
        assert_eq!(
            sub_projected,
            RecordBatch::try_new(
                expected_schema,
                vec![Arc::new(Int32Array::from_iter_values(0..20))],
            )
            .unwrap()
        );
    }

    #[test]
    fn test_project_rebuilds_sliced_map() {
        // A sliced MapArray keeps its full entries array behind sliced
        // offsets and validity; the Map projection arm must rebuild it
        // without renormalizing either.
        let entry_fields = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, true),
        ]);
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(entry_fields.clone()),
            false,
        ));
        let entries = StructArray::new(
            entry_fields,
            vec![
                Arc::new(StringArray::from(vec!["k0", "k1", "k2"])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
            None,
        );
        let map = MapArray::new(
            entries_field.clone(),
            OffsetBuffer::new(vec![0, 1, 1, 3].into()),
            entries,
            Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
            false,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(entries_field, false),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(map) as ArrayRef]).unwrap();

        // Rows 1..3: a null slot and a two-entry slot, offsets not zero-based.
        let projected = batch
            .slice(1, 2)
            .project_by_schema(schema.as_ref())
            .unwrap();
        let map = projected.column(0).as_map();
        assert!(map.is_null(0));
        assert!(map.is_valid(1));
        assert_eq!(map.value_length(1), 2);
        assert_eq!(
            map.value(1)
                .column(1)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values(),
            &[2, 3]
        );
    }

    #[test]
    fn test_project_preserves_struct_validity() {
        // Test that projecting a struct array preserves its validity (fix for issue #4385)
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float32, true),
        ]);

        // Create a struct array with validity
        let id_array = Int32Array::from(vec![1, 2, 3]);
        let value_array = Float32Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]);
        let struct_array = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(id_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
            Some(vec![true, false, true].into()), // Second struct is null
        );

        // Project the struct array
        let projected = project(&struct_array, &fields).unwrap();

        // Verify the validity is preserved
        assert_eq!(projected.null_count(), 1);
        assert!(!projected.is_null(0));
        assert!(projected.is_null(1));
        assert!(!projected.is_null(2));
    }

    #[test]
    fn test_merge_struct_with_different_validity() {
        // Test case from Weston's review comment
        // File 1 has height field with some nulls
        let height_array = Int32Array::from(vec![Some(500), None, Some(600), None]);
        let left_fields = Fields::from(vec![Field::new("height", DataType::Int32, true)]);
        let left_struct = StructArray::new(
            left_fields,
            vec![Arc::new(height_array) as ArrayRef],
            Some(vec![true, false, true, false].into()), // Rows 2 and 4 are null structs
        );

        // File 2 has width field with some nulls
        let width_array = Int32Array::from(vec![Some(300), Some(200), None, None]);
        let right_fields = Fields::from(vec![Field::new("width", DataType::Int32, true)]);
        let right_struct = StructArray::new(
            right_fields,
            vec![Arc::new(width_array) as ArrayRef],
            Some(vec![true, true, false, false].into()), // Rows 3 and 4 are null structs
        );

        // Merge the two structs
        let merged = merge(&left_struct, &right_struct).unwrap();

        // Expected:
        // Row 1: both non-null -> {width: 300, height: 500}
        // Row 2: left null, right non-null -> {width: 200, height: null}
        // Row 3: left non-null, right null -> {width: null, height: 600}
        // Row 4: both null -> null struct

        assert_eq!(merged.null_count(), 1); // Only row 4 is null
        assert!(!merged.is_null(0));
        assert!(!merged.is_null(1));
        assert!(!merged.is_null(2));
        assert!(merged.is_null(3));

        // Check field values
        let height_col = merged.column_by_name("height").unwrap();
        let height_values = height_col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(height_values.value(0), 500);
        assert!(height_values.is_null(1)); // height is null when left struct was null
        assert_eq!(height_values.value(2), 600);

        let width_col = merged.column_by_name("width").unwrap();
        let width_values = width_col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(width_values.value(0), 300);
        assert_eq!(width_values.value(1), 200);
        assert!(width_values.is_null(2)); // width is null when right struct was null

        // An all-null validity buffer is data, not a placeholder meaning "this side has no
        // validity": merging two of them keeps the rows null.
        let all_null_left = StructArray::new(
            Fields::from(vec![Field::new("height", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![None, None])) as ArrayRef],
            Some(vec![false, false].into()),
        );
        let all_null_right = StructArray::new(
            Fields::from(vec![Field::new("width", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![None, None])) as ArrayRef],
            Some(vec![false, false].into()),
        );

        let merged = merge(&all_null_left, &all_null_right).unwrap();
        assert_eq!(merged.null_count(), 2);

        // An all-null side is the identity of the merge, so the other side decides each row.
        let partial_left = StructArray::new(
            Fields::from(vec![Field::new("height", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
            Some(vec![true, false].into()),
        );
        let merged = merge(&partial_left, &all_null_right).unwrap();
        assert!(!merged.is_null(0));
        assert!(merged.is_null(1));

        // A missing validity buffer means all rows are valid, which absorbs an all-null side.
        let all_valid_left = StructArray::new(
            Fields::from(vec![Field::new("height", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
            None,
        );
        let merged = merge(&all_valid_left, &all_null_right).unwrap();
        assert_eq!(merged.null_count(), 0);

        // An explicit all-valid buffer has the same semantics as a missing buffer.
        let all_valid: arrow_buffer::NullBuffer = vec![true, true].into();
        let partial: arrow_buffer::NullBuffer = vec![true, false].into();
        assert!(merge_struct_validity(Some(&all_valid), Some(&partial)).is_none());
        assert!(merge_struct_validity(Some(&partial), Some(&all_valid)).is_none());
    }

    #[test]
    fn test_merge_null_typed_column_with_parent_validity() {
        // Reproduces ENT-990: panic in adjust_child_validity when a Null-typed column
        // exists on one side and the parent struct has null rows.
        // Arrow's Null type has no null bitmap, so passing one to ArrayData::try_new panics.
        let left_struct = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
            Some(vec![true, false].into()),
        );
        let right_struct = StructArray::new(
            Fields::from(vec![Field::new("b", DataType::Null, true)]),
            vec![Arc::new(NullArray::new(2)) as ArrayRef],
            Some(vec![true, false].into()),
        );

        // Previously panicked: "Arrays of type Null cannot contain a null bitmask"
        let merged = merge(&left_struct, &right_struct).unwrap();
        assert_eq!(merged.len(), 2);
        let b_col = merged.column_by_name("b").unwrap();
        // DataType::Null implies all-null by definition; no null bitmap is stored.
        assert_eq!(b_col.data_type(), &DataType::Null);
        assert_eq!(b_col.len(), 2);
    }

    #[test]
    fn test_merge_with_schema_with_nullable_struct_list_schema_mismatch() {
        // left_list setup
        let left_company_id = Arc::new(Int32Array::from(vec![None, None]));
        let left_count = Arc::new(Int32Array::from(vec![None, None]));
        let left_struct = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("company_id", DataType::Int32, true),
                Field::new("count", DataType::Int32, true),
            ]),
            vec![left_company_id, left_count],
            None,
        ));
        let left_list = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(left_struct.fields().clone()),
                true,
            )),
            OffsetBuffer::from_lengths([2]),
            left_struct,
            None,
        ));

        // Right List Setup
        let right_company_name = Arc::new(StringArray::from(vec!["Google", "Microsoft"]));
        let right_struct = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("company_name", DataType::Utf8, true)]),
            vec![right_company_name],
            None,
        ));
        let right_list = Arc::new(ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(right_struct.fields().clone()),
                true,
            )),
            OffsetBuffer::from_lengths([2]),
            right_struct,
            None,
        ));

        let target_fields = Fields::from(vec![Field::new(
            "companies",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("company_id", DataType::Int32, true),
                    Field::new("company_name", DataType::Utf8, true),
                    Field::new("count", DataType::Int32, true),
                ])),
                true,
            ))),
            true,
        )]);

        let left_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "companies",
                left_list.data_type().clone(),
                true,
            )])),
            vec![left_list as ArrayRef],
        )
        .unwrap();

        let right_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "companies",
                right_list.data_type().clone(),
                true,
            )])),
            vec![right_list as ArrayRef],
        )
        .unwrap();

        let merged = left_batch
            .merge_with_schema(&right_batch, &Schema::new(target_fields.to_vec()))
            .unwrap();

        // Verify the merged structure
        let merged_list = merged
            .column_by_name("companies")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let merged_struct = merged_list.values().as_struct();

        // Should have all 3 fields
        assert_eq!(merged_struct.num_columns(), 3);
        assert!(merged_struct.column_by_name("company_id").is_some());
        assert!(merged_struct.column_by_name("company_name").is_some());
        assert!(merged_struct.column_by_name("count").is_some());

        // Verify values
        let company_id = merged_struct
            .column_by_name("company_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(company_id.is_null(0));
        assert!(company_id.is_null(1));

        let company_name = merged_struct
            .column_by_name("company_name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(company_name.value(0), "Google");
        assert_eq!(company_name.value(1), "Microsoft");

        let count = merged_struct
            .column_by_name("count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(count.is_null(0));
        assert!(count.is_null(1));
    }

    #[test]
    fn test_merge_struct_lists() {
        test_merge_struct_lists_generic::<i32>();
    }

    #[test]
    fn test_merge_struct_large_lists() {
        test_merge_struct_lists_generic::<i64>();
    }

    fn test_merge_struct_lists_generic<O: OffsetSizeTrait>() {
        // left_list setup
        let left_company_id = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            Some(15),
            Some(16),
            Some(17),
            Some(18),
            Some(19),
            Some(20),
        ]));
        let left_count = Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            Some(50),
            Some(60),
            Some(70),
            Some(80),
            Some(90),
            Some(100),
            Some(110),
            Some(120),
            Some(130),
            Some(140),
            Some(150),
            Some(160),
            Some(170),
            Some(180),
            Some(190),
            Some(200),
        ]));
        let left_struct = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("company_id", DataType::Int32, true),
                Field::new("count", DataType::Int32, true),
            ]),
            vec![left_company_id, left_count],
            None,
        ));

        let left_list = Arc::new(GenericListArray::<O>::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(left_struct.fields().clone()),
                true,
            )),
            OffsetBuffer::from_lengths([3, 1]),
            left_struct.clone(),
            None,
        ));

        let left_list_struct = Arc::new(StructArray::new(
            Fields::from(vec![Field::new(
                "companies",
                if O::IS_LARGE {
                    DataType::LargeList(Arc::new(Field::new(
                        "item",
                        DataType::Struct(left_struct.fields().clone()),
                        true,
                    )))
                } else {
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(left_struct.fields().clone()),
                        true,
                    )))
                },
                true,
            )]),
            vec![left_list as ArrayRef],
            None,
        ));

        // right_list setup
        let right_company_name = Arc::new(StringArray::from(vec![
            "Google",
            "Microsoft",
            "Apple",
            "Facebook",
        ]));
        let right_struct = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("company_name", DataType::Utf8, true)]),
            vec![right_company_name],
            None,
        ));
        let right_list = Arc::new(GenericListArray::<O>::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(right_struct.fields().clone()),
                true,
            )),
            OffsetBuffer::from_lengths([3, 1]),
            right_struct.clone(),
            None,
        ));

        let right_list_struct = Arc::new(StructArray::new(
            Fields::from(vec![Field::new(
                "companies",
                if O::IS_LARGE {
                    DataType::LargeList(Arc::new(Field::new(
                        "item",
                        DataType::Struct(right_struct.fields().clone()),
                        true,
                    )))
                } else {
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Struct(right_struct.fields().clone()),
                        true,
                    )))
                },
                true,
            )]),
            vec![right_list as ArrayRef],
            None,
        ));

        // prepare schema
        let target_fields = Fields::from(vec![Field::new(
            "companies",
            if O::IS_LARGE {
                DataType::LargeList(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("company_id", DataType::Int32, true),
                        Field::new("company_name", DataType::Utf8, true),
                        Field::new("count", DataType::Int32, true),
                    ])),
                    true,
                )))
            } else {
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(Fields::from(vec![
                        Field::new("company_id", DataType::Int32, true),
                        Field::new("company_name", DataType::Utf8, true),
                        Field::new("count", DataType::Int32, true),
                    ])),
                    true,
                )))
            },
            true,
        )]);

        // merge left_list and right_list
        let merged_array = merge_with_schema(&left_list_struct, &right_list_struct, &target_fields);
        assert_eq!(merged_array.len(), 2);
    }

    #[test]
    fn test_merge_with_schema_sliced_list_struct() {
        test_merge_with_schema_sliced_list_struct_generic::<i32>();
    }

    #[test]
    fn test_merge_with_schema_sliced_large_list_struct() {
        test_merge_with_schema_sliced_list_struct_generic::<i64>();
    }

    // Regression for #6580: merge_with_schema panicked when the left list was a
    // sliced view whose offsets did not start at zero (common after a filtered
    // scan). Cloning those offsets alongside `trimmed_values` produced offsets
    // larger than the trimmed child, panicking in `(Large)ListArray::new`.
    fn test_merge_with_schema_sliced_list_struct_generic<O: OffsetSizeTrait>() {
        let make_list_dtype = |item_field: Arc<Field>| {
            if O::IS_LARGE {
                DataType::LargeList(item_field)
            } else {
                DataType::List(item_field)
            }
        };

        // Build a List<Struct> with two rows of 5 items each, then slice away
        // the first row so the remaining list's offsets start at 5, not 0.
        let struct_fields_a = Fields::from(vec![Field::new("a", DataType::Int32, true)]);
        let left_values = Arc::new(StructArray::new(
            struct_fields_a.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10)) as ArrayRef],
            None,
        ));
        let full_list = GenericListArray::<O>::new(
            Arc::new(Field::new("item", DataType::Struct(struct_fields_a), true)),
            OffsetBuffer::<O>::from_lengths([5, 5]),
            left_values,
            None,
        );
        let sliced_left = full_list.slice(1, 1);
        assert_eq!(sliced_left.offsets()[0].as_usize(), 5);
        assert_eq!(sliced_left.offsets()[1].as_usize(), 10);

        let struct_fields_b = Fields::from(vec![Field::new("b", DataType::Int32, true)]);
        let right_values = Arc::new(StructArray::new(
            struct_fields_b.clone(),
            vec![Arc::new(Int32Array::from_iter_values(100..105)) as ArrayRef],
            None,
        ));
        let right_list = GenericListArray::<O>::new(
            Arc::new(Field::new("item", DataType::Struct(struct_fields_b), true)),
            OffsetBuffer::<O>::from_lengths([5]),
            right_values,
            None,
        );

        let target_item_field = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ])),
            true,
        ));
        let target_fields = Fields::from(vec![Field::new(
            "items",
            make_list_dtype(target_item_field),
            true,
        )]);

        let left_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                sliced_left.data_type().clone(),
                true,
            )])),
            vec![Arc::new(sliced_left) as ArrayRef],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                right_list.data_type().clone(),
                true,
            )])),
            vec![Arc::new(right_list) as ArrayRef],
        )
        .unwrap();

        let merged = left_batch
            .merge_with_schema(&right_batch, &Schema::new(target_fields.to_vec()))
            .unwrap();

        let merged_list = merged
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .unwrap();
        assert_eq!(merged_list.len(), 1);
        assert_eq!(merged_list.value_length(0).as_usize(), 5);
        let merged_struct = merged_list.values().as_struct();
        assert_eq!(merged_struct.num_columns(), 2);
        let a = merged_struct
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // After shifting offsets to zero, values 5..10 should be first.
        let a_vals: Vec<i32> = a.iter().map(|v| v.unwrap()).collect();
        assert_eq!(a_vals, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_project_by_schema_list_struct_reorder() {
        // Test that project_by_schema correctly reorders fields inside List<Struct>
        // This is a regression test for issue #5702

        // Source schema with inner struct fields in order: c, b, a
        let source_inner_struct = DataType::Struct(Fields::from(vec![
            Field::new("c", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Utf8, true),
        ]));
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new(
                    "item",
                    source_inner_struct.clone(),
                    true,
                ))),
                true,
            ),
        ]));

        // Create source data with c, b, a order
        let c_array = StringArray::from(vec!["c1", "c2"]);
        let b_array = StringArray::from(vec!["b1", "b2"]);
        let a_array = StringArray::from(vec!["a1", "a2"]);
        let inner_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("c", DataType::Utf8, true)),
                Arc::new(c_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(b_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(a_array) as ArrayRef,
            ),
        ]);

        let list_array = ListArray::new(
            Arc::new(Field::new("item", source_inner_struct, true)),
            OffsetBuffer::from_lengths([1, 1]),
            Arc::new(inner_struct),
            None,
        );

        let batch = RecordBatch::try_new(
            source_schema,
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(list_array)],
        )
        .unwrap();

        // Target schema with inner struct fields in order: a, b, c
        let target_inner_struct = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Utf8, true),
        ]));
        let target_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", target_inner_struct, true))),
                true,
            ),
        ]);

        // Project should reorder the inner struct fields
        let projected = batch.project_by_schema(&target_schema).unwrap();

        // Verify the schema is correct
        assert_eq!(projected.schema().as_ref(), &target_schema);

        // Verify the data is correct by checking inner struct field order
        let projected_list = projected.column(1).as_list::<i32>();
        let projected_struct = projected_list.values().as_struct();

        // Fields should now be in order: a, b, c
        assert_eq!(
            projected_struct.column_by_name("a").unwrap().as_ref(),
            &StringArray::from(vec!["a1", "a2"]) as &dyn Array
        );
        assert_eq!(
            projected_struct.column_by_name("b").unwrap().as_ref(),
            &StringArray::from(vec!["b1", "b2"]) as &dyn Array
        );
        assert_eq!(
            projected_struct.column_by_name("c").unwrap().as_ref(),
            &StringArray::from(vec!["c1", "c2"]) as &dyn Array
        );

        // Also verify positional access matches expected order (a=0, b=1, c=2)
        assert_eq!(
            projected_struct.column(0).as_ref(),
            &StringArray::from(vec!["a1", "a2"]) as &dyn Array
        );
        assert_eq!(
            projected_struct.column(1).as_ref(),
            &StringArray::from(vec!["b1", "b2"]) as &dyn Array
        );
        assert_eq!(
            projected_struct.column(2).as_ref(),
            &StringArray::from(vec!["c1", "c2"]) as &dyn Array
        );
    }

    #[test]
    fn test_project_by_schema_nested_list_struct() {
        // Test deeply nested List<Struct<List<Struct>>> projection
        let inner_struct = DataType::Struct(Fields::from(vec![
            Field::new("y", DataType::Int32, true),
            Field::new("x", DataType::Int32, true),
        ]));
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("b", DataType::Utf8, true),
                    Field::new(
                        "inner_list",
                        DataType::List(Arc::new(Field::new("item", inner_struct.clone(), true))),
                        true,
                    ),
                    Field::new("a", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        )]));

        // Create deeply nested data
        let y_array = Int32Array::from(vec![1, 2]);
        let x_array = Int32Array::from(vec![3, 4]);
        let innermost_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("y", DataType::Int32, true)),
                Arc::new(y_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("x", DataType::Int32, true)),
                Arc::new(x_array) as ArrayRef,
            ),
        ]);
        let inner_list = ListArray::new(
            Arc::new(Field::new("item", inner_struct.clone(), true)),
            OffsetBuffer::from_lengths([2]),
            Arc::new(innermost_struct),
            None,
        );

        let b_array = StringArray::from(vec!["b1"]);
        let a_array = StringArray::from(vec!["a1"]);
        let middle_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Utf8, true)),
                Arc::new(b_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "inner_list",
                    DataType::List(Arc::new(Field::new("item", inner_struct, true))),
                    true,
                )),
                Arc::new(inner_list) as ArrayRef,
            ),
            (
                Arc::new(Field::new("a", DataType::Utf8, true)),
                Arc::new(a_array) as ArrayRef,
            ),
        ]);

        let outer_list = ListArray::new(
            Arc::new(Field::new("item", middle_struct.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1]),
            Arc::new(middle_struct),
            None,
        );

        let batch =
            RecordBatch::try_new(source_schema, vec![Arc::new(outer_list) as ArrayRef]).unwrap();

        // Target schema with reordered fields at all levels
        let target_inner_struct = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, true), // x before y now
            Field::new("y", DataType::Int32, true),
        ]));
        let target_schema = Schema::new(vec![Field::new(
            "outer",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Utf8, true), // a before b now
                    Field::new(
                        "inner_list",
                        DataType::List(Arc::new(Field::new("item", target_inner_struct, true))),
                        true,
                    ),
                    Field::new("b", DataType::Utf8, true),
                ])),
                true,
            ))),
            true,
        )]);

        let projected = batch.project_by_schema(&target_schema).unwrap();

        // Verify schema
        assert_eq!(projected.schema().as_ref(), &target_schema);

        // Verify deeply nested data is reordered correctly
        let outer_list = projected.column(0).as_list::<i32>();
        let middle_struct = outer_list.values().as_struct();

        // Middle struct should have a first, then inner_list, then b
        assert_eq!(
            middle_struct.column(0).as_ref(),
            &StringArray::from(vec!["a1"]) as &dyn Array
        );
        assert_eq!(
            middle_struct.column(2).as_ref(),
            &StringArray::from(vec!["b1"]) as &dyn Array
        );

        // Inner list's struct should have x first, then y
        let inner_list = middle_struct.column(1).as_list::<i32>();
        let innermost_struct = inner_list.values().as_struct();
        assert_eq!(
            innermost_struct.column(0).as_ref(),
            &Int32Array::from(vec![3, 4]) as &dyn Array
        );
        assert_eq!(
            innermost_struct.column(1).as_ref(),
            &Int32Array::from(vec![1, 2]) as &dyn Array
        );
    }

    #[test]
    fn test_iter_binary_array_accepts_binary_variants() {
        let binary = BinaryArray::from(vec![b"a".as_slice(), b"bc"]);
        assert_eq!(
            iter_binary_array(&binary).unwrap().collect::<Vec<_>>(),
            vec![Some(b"a".as_slice()), Some(b"bc".as_slice())]
        );

        let large_binary = LargeBinaryArray::from(vec![b"x".as_slice(), b"yz"]);
        assert_eq!(
            iter_binary_array(&large_binary)
                .unwrap()
                .collect::<Vec<_>>(),
            vec![Some(b"x".as_slice()), Some(b"yz".as_slice())]
        );

        let binary_view = BinaryViewArray::from(vec![b"1".as_slice(), b"23"]);
        assert_eq!(
            iter_binary_array(&binary_view).unwrap().collect::<Vec<_>>(),
            vec![Some(b"1".as_slice()), Some(b"23".as_slice())]
        );

        let fixed_size =
            FixedSizeBinaryArray::try_from_iter([b"abcd", b"efgh"].into_iter()).unwrap();
        assert_eq!(
            iter_binary_array(&fixed_size).unwrap().collect::<Vec<_>>(),
            vec![Some(b"abcd".as_slice()), Some(b"efgh".as_slice())]
        );
    }

    #[test]
    fn test_iter_binary_array_rejects_non_binary() {
        let int_array = Int32Array::from(vec![1, 2, 3]);
        let Err(error) = iter_binary_array(&int_array) else {
            panic!("expected an error for non-binary array");
        };
        assert!(error.to_string().contains("Expecting a binary type"));
    }
}
