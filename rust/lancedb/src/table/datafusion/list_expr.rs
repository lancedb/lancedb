// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Applies the write path's coercion to the values inside a list column.
//!
//! Most coercions are expressible with the expressions DataFusion ships, but nothing there
//! reaches inside a list: `get_field` reads a struct child, and there is no counterpart that
//! rebuilds a list from coerced values. So a list whose items need synthesizing rather than
//! casting — a blob column inside a list, say — needs an expression of its own.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, ListArray, RecordBatch, cast::AsArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion_common::{DataFusionError, Result as DFResult, exec_err};
use datafusion_expr::ColumnarValue;
use datafusion_physical_plan::PhysicalExpr;

/// How the list holding the coerced values is put back together.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ListKind {
    List,
    LargeList,
    FixedSize(i32),
}

impl ListKind {
    fn of(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::List(_) => Some(Self::List),
            DataType::LargeList(_) => Some(Self::LargeList),
            DataType::FixedSizeList(_, len) => Some(Self::FixedSize(*len)),
            _ => None,
        }
    }
}

/// Rebuilds a list column with its values passed through `values`.
///
/// The list's offsets and validity are carried over untouched, so only the items change.
#[derive(Debug)]
pub(super) struct CoerceListValues {
    input: Arc<dyn PhysicalExpr>,
    /// Coerces the items. Evaluated against a batch of the input's values, as a single
    /// column named after the input's item field.
    values: Arc<dyn PhysicalExpr>,
    input_item: FieldRef,
    output_field: FieldRef,
    kind: ListKind,
}

impl CoerceListValues {
    /// `output_field` must be a list field of the same kind as the input's, whose item field
    /// is what `values` returns: the offsets and validity are the input's, so they only fit
    /// a list of the kind they came from. Converting `List` to `FixedSizeList` and the like
    /// is left to a cast over the result, once the items are the right type for it.
    pub(super) fn new(
        input: Arc<dyn PhysicalExpr>,
        values: Arc<dyn PhysicalExpr>,
        input_item: FieldRef,
        output_field: FieldRef,
    ) -> DFResult<Self> {
        let Some(kind) = ListKind::of(output_field.data_type()) else {
            return exec_err!(
                "cannot coerce list values into non-list field '{}' of type {}",
                output_field.name(),
                output_field.data_type()
            );
        };
        Ok(Self {
            input,
            values,
            input_item,
            output_field,
            kind,
        })
    }

    /// The list's values, windowed to the slice the offsets actually address, and offsets
    /// starting at zero.
    ///
    /// Read off the child array rather than through `list_flatten`, which drops the values a
    /// null slot spans and so would leave the offsets pointing at the wrong items.
    fn values_of(&self, list: &dyn Array) -> DFResult<(ArrayRef, Offsets)> {
        match self.kind {
            ListKind::List => {
                let list = list.as_list::<i32>();
                let offsets = list.offsets();
                let first = offsets[0] as usize;
                let last = offsets[offsets.len() - 1] as usize;
                if first == 0 {
                    return Ok((list.values().slice(0, last), Offsets::I32(offsets.clone())));
                }
                Ok((
                    list.values().slice(first, last - first),
                    Offsets::I32(rebase_i32(offsets, first)),
                ))
            }
            ListKind::LargeList => {
                let list = list.as_list::<i64>();
                let offsets = list.offsets();
                let first = offsets[0] as usize;
                let last = offsets[offsets.len() - 1] as usize;
                if first == 0 {
                    return Ok((list.values().slice(0, last), Offsets::I64(offsets.clone())));
                }
                Ok((
                    list.values().slice(first, last - first),
                    Offsets::I64(rebase_i64(offsets, first)),
                ))
            }
            ListKind::FixedSize(len) => {
                let list = list.as_fixed_size_list();
                let len = len as usize;
                Ok((
                    list.values().slice(list.offset() * len, list.len() * len),
                    Offsets::None,
                ))
            }
        }
    }

    fn coerce_values(&self, values: &ArrayRef) -> DFResult<ArrayRef> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![self.input_item.clone()])),
            vec![values.clone()],
        )?;
        self.values.evaluate(&batch)?.into_array(values.len())
    }

    fn item_field(&self) -> DFResult<FieldRef> {
        match self.output_field.data_type() {
            DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
                Ok(item.clone())
            }
            other => exec_err!("expected a list field, got {other}"),
        }
    }
}

/// The offsets to rebuild the list with, absent for a fixed-size list.
enum Offsets {
    I32(OffsetBuffer<i32>),
    I64(OffsetBuffer<i64>),
    None,
}

fn rebase_i32(offsets: &OffsetBuffer<i32>, first: usize) -> OffsetBuffer<i32> {
    let first = first as i32;
    OffsetBuffer::new(offsets.iter().map(|o| *o - first).collect())
}

fn rebase_i64(offsets: &OffsetBuffer<i64>, first: usize) -> OffsetBuffer<i64> {
    let first = first as i64;
    OffsetBuffer::new(offsets.iter().map(|o| *o - first).collect())
}

impl fmt::Display for CoerceListValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "coerce_list_values({}, {})",
            self.output_field.name(),
            self.values
        )
    }
}

impl PartialEq for CoerceListValues {
    fn eq(&self, other: &Self) -> bool {
        self.input.eq(&other.input)
            && self.values.eq(&other.values)
            && self.input_item == other.input_item
            && self.output_field == other.output_field
            && self.kind == other.kind
    }
}

impl Eq for CoerceListValues {}

impl Hash for CoerceListValues {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.values.hash(state);
        self.input_item.hash(state);
        self.output_field.hash(state);
        self.kind.hash(state);
    }
}

impl PhysicalExpr for CoerceListValues {
    fn return_field(&self, _input_schema: &Schema) -> DFResult<FieldRef> {
        Ok(self.output_field.clone())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        let list = self.input.evaluate(batch)?.into_array(batch.num_rows())?;
        let (values, offsets) = self.values_of(list.as_ref())?;
        let coerced = self.coerce_values(&values)?;
        let item_field = self.item_field()?;
        let nulls = list.nulls().cloned();

        let rebuilt: ArrayRef = match (&self.kind, offsets) {
            (ListKind::List, Offsets::I32(offsets)) => {
                Arc::new(ListArray::try_new(item_field, offsets, coerced, nulls)?)
            }
            (ListKind::LargeList, Offsets::I64(offsets)) => Arc::new(LargeListArray::try_new(
                item_field, offsets, coerced, nulls,
            )?),
            (ListKind::FixedSize(len), Offsets::None) => Arc::new(FixedSizeListArray::try_new(
                item_field, *len, coerced, nulls,
            )?),
            (kind, _) => {
                return Err(DataFusionError::Internal(format!(
                    "list kind {kind:?} paired with the wrong offsets"
                )));
            }
        };
        Ok(ColumnarValue::Array(rebuilt))
    }

    /// Only the list column is a child. The values expression reads a synthetic one-column
    /// batch of the list's items, not this expression's input, so exposing it would invite
    /// the planner to resolve its column references against the wrong schema.
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let [input] = <[_; 1]>::try_from(children).map_err(|children: Vec<_>| {
            DataFusionError::Internal(format!(
                "coerce_list_values expects one child, got {}",
                children.len()
            ))
        })?;
        Ok(Arc::new(Self::new(
            input,
            self.values.clone(),
            self.input_item.clone(),
            self.output_field.clone(),
        )?))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// The item field of `data_type`, if it is a list of some kind.
pub(super) fn list_item(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(item) | DataType::LargeList(item) | DataType::FixedSizeList(item, _) => {
            Some(item)
        }
        _ => None,
    }
}

/// Whether two list types are the same kind of list, ignoring their items.
pub(super) fn same_list_kind(left: &DataType, right: &DataType) -> bool {
    match (ListKind::of(left), ListKind::of(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// `named_like`'s name, nullability and metadata, holding `item`s in a list shaped like
/// `shaped_like`.
pub(super) fn list_of(
    named_like: &Field,
    shaped_like: &DataType,
    item: FieldRef,
) -> DFResult<Field> {
    let data_type = match shaped_like {
        DataType::List(_) => DataType::List(item),
        DataType::LargeList(_) => DataType::LargeList(item),
        DataType::FixedSizeList(_, len) => DataType::FixedSizeList(item, *len),
        other => return exec_err!("expected a list type, got {other}"),
    };
    Ok(
        Field::new(named_like.name(), data_type, named_like.is_nullable())
            .with_metadata(named_like.metadata().clone()),
    )
}
