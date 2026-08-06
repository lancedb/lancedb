// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::scalar::rtree::sort::Sorter;
use arrow_array::{ArrayRef, UInt32Array};
use arrow_schema::{ArrowError, DataType as ArrowDataType, Field as ArrowField, Field};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_physical_expr::expressions::Column as DFColumn;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use lance_core::Result;
use lance_datafusion::exec::{LanceExecutionOptions, OneShotExec, execute_plan};
use lance_geo::bbox::{BoundingBox, bounding_box};
use std::sync::Arc;

const HILBERT_FIELD_NAME: &str = "_hilbert";

pub struct HilbertSorter {
    bbox: BoundingBox,
}

impl HilbertSorter {
    pub fn new(bbox: BoundingBox) -> Self {
        Self { bbox }
    }
}

#[async_trait]
impl Sorter for HilbertSorter {
    async fn sort(&self, data: SendableRecordBatchStream) -> Result<SendableRecordBatchStream> {
        let data_schema = data.schema();
        let bbox_field = data_schema.field(0).clone();
        let source = Arc::new(OneShotExec::new(data));

        // 1. Add _hilbert column
        let mut projection_exprs = data_schema
            .fields()
            .iter()
            .map(|f| f.name())
            .enumerate()
            .map(|(idx, field_name)| {
                (
                    Arc::new(DFColumn::new(field_name, idx)) as Arc<dyn PhysicalExpr>,
                    field_name.clone(),
                )
            })
            .collect::<Vec<_>>();
        projection_exprs.push((
            HilbertUDF::new(self.bbox, bbox_field).into_physical_expr(),
            HILBERT_FIELD_NAME.to_string(),
        ));

        let projection = Arc::new(ProjectionExec::try_new(
            projection_exprs,
            source as Arc<dyn ExecutionPlan>,
        )?);

        // 2. sort_by _hilbert
        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(DFColumn::new(HILBERT_FIELD_NAME, 2)), // _hilbert column
            options: arrow_schema::SortOptions::default(),
        };

        let sort_exec = Arc::new(SortExec::new(
            [sort_expr].into(),
            projection as Arc<dyn ExecutionPlan>,
        ));

        let sorted_stream = execute_plan(
            sort_exec,
            LanceExecutionOptions {
                use_spilling: true,
                ..Default::default()
            },
        )?;

        Ok(sorted_stream)
    }
}

const HILBERT_UDF_NAME: &str = "hilbert";

#[derive(Debug, Clone)]
struct HilbertUDF {
    signature: Signature,
    bbox: BoundingBox,
    bbox_field: Field,
}

impl PartialEq for HilbertUDF {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && self.bbox.minx() == other.bbox.minx()
            && self.bbox.miny() == other.bbox.miny()
            && self.bbox.maxx() == other.bbox.maxx()
            && self.bbox.maxy() == other.bbox.maxy()
            && self.bbox_field == other.bbox_field
    }
}

impl Eq for HilbertUDF {}

impl std::hash::Hash for HilbertUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.bbox.minx().to_bits().hash(state);
        self.bbox.miny().to_bits().hash(state);
        self.bbox.maxx().to_bits().hash(state);
        self.bbox.maxy().to_bits().hash(state);
        self.bbox_field.hash(state);
    }
}

impl HilbertUDF {
    fn new(bbox: BoundingBox, bbox_field: Field) -> Self {
        let signature =
            Signature::exact(vec![bbox_field.data_type().clone()], Volatility::Immutable);
        Self {
            signature,
            bbox,
            bbox_field,
        }
    }

    fn into_physical_expr(self) -> Arc<dyn PhysicalExpr> {
        Arc::new(ScalarFunctionExpr::new(
            HILBERT_UDF_NAME,
            Arc::new(self.into()),
            vec![Arc::new(DFColumn::new("bbox", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(ArrowField::new(
                HILBERT_FIELD_NAME,
                ArrowDataType::UInt32,
                false,
            )),
            Arc::new(ConfigOptions::default()),
        ))
    }
}

impl ScalarUDFImpl for HilbertUDF {
    fn name(&self) -> &str {
        HILBERT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[ArrowDataType]) -> DataFusionResult<ArrowDataType> {
        Ok(ArrowDataType::UInt32)
    }

    fn invoke_with_args(&self, func_args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let value = match &func_args.args[0] {
            ColumnarValue::Array(array) => from_arrow_array(array.as_ref(), &self.bbox_field)
                .map_err(|e| DataFusionError::from(ArrowError::from(e))),
            _ => Err(DataFusionError::Execution(
                "hilbert only supports array arguments".to_owned(),
            )),
        }?;

        let rect_array = bounding_box(value.as_ref()).map_err(DataFusionError::from)?;

        let hilbert_max = ((1 << 16) - 1) as f64;
        let len = rect_array.len();
        let width = self.bbox.maxx() - self.bbox.minx();
        let width = if width == 0.0 { 1.0 } else { width };
        let height = self.bbox.maxy() - self.bbox.miny();
        let height = if height == 0.0 { 1.0 } else { height };
        let mut hilbert_values = Vec::with_capacity(len);
        for r in rect_array.iter().flatten() {
            let mut bbox = BoundingBox::new();
            let r = r.map_err(|e| DataFusionError::from(ArrowError::from(e)))?;
            bbox.add_geometry(&r);
            let x = (hilbert_max * ((bbox.minx() + bbox.maxx()) / 2. - self.bbox.minx()) / width)
                .floor() as u32;
            let y = (hilbert_max * ((bbox.miny() + bbox.maxy()) / 2. - self.bbox.miny()) / height)
                .floor() as u32;
            hilbert_values.push(hilbert_curve(x, y));
        }

        Ok(ColumnarValue::Array(
            Arc::new(UInt32Array::from(hilbert_values)) as ArrayRef,
        ))
    }
}

/// Fast Hilbert curve algorithm by http://threadlocalmutex.com/
/// Ported from https://github.com/kylebarron/geo-index
#[inline]
fn hilbert_curve(x: u32, y: u32) -> u32 {
    let mut a_1 = x ^ y;
    let mut b_1 = 0xFFFF ^ a_1;
    let mut c_1 = 0xFFFF ^ (x | y);
    let mut d_1 = x & (y ^ 0xFFFF);

    let mut a_2 = a_1 | (b_1 >> 1);
    let mut b_2 = (a_1 >> 1) ^ a_1;
    let mut c_2 = ((c_1 >> 1) ^ (b_1 & (d_1 >> 1))) ^ c_1;
    let mut d_2 = ((a_1 & (c_1 >> 1)) ^ (d_1 >> 1)) ^ d_1;

    a_1 = a_2;
    b_1 = b_2;
    c_1 = c_2;
    d_1 = d_2;
    a_2 = (a_1 & (a_1 >> 2)) ^ (b_1 & (b_1 >> 2));
    b_2 = (a_1 & (b_1 >> 2)) ^ (b_1 & ((a_1 ^ b_1) >> 2));
    c_2 ^= (a_1 & (c_1 >> 2)) ^ (b_1 & (d_1 >> 2));
    d_2 ^= (b_1 & (c_1 >> 2)) ^ ((a_1 ^ b_1) & (d_1 >> 2));

    a_1 = a_2;
    b_1 = b_2;
    c_1 = c_2;
    d_1 = d_2;
    a_2 = (a_1 & (a_1 >> 4)) ^ (b_1 & (b_1 >> 4));
    b_2 = (a_1 & (b_1 >> 4)) ^ (b_1 & ((a_1 ^ b_1) >> 4));
    c_2 ^= (a_1 & (c_1 >> 4)) ^ (b_1 & (d_1 >> 4));
    d_2 ^= (b_1 & (c_1 >> 4)) ^ ((a_1 ^ b_1) & (d_1 >> 4));

    a_1 = a_2;
    b_1 = b_2;
    c_1 = c_2;
    d_1 = d_2;
    c_2 ^= (a_1 & (c_1 >> 8)) ^ (b_1 & (d_1 >> 8));
    d_2 ^= (b_1 & (c_1 >> 8)) ^ ((a_1 ^ b_1) & (d_1 >> 8));

    a_1 = c_2 ^ (c_2 >> 1);
    b_1 = d_2 ^ (d_2 >> 1);

    let mut i0 = x ^ y;
    let mut i1 = b_1 | (0xFFFF ^ (i0 | a_1));

    i0 = (i0 | (i0 << 8)) & 0x00FF_00FF;
    i0 = (i0 | (i0 << 4)) & 0x0F0F_0F0F;
    i0 = (i0 | (i0 << 2)) & 0x3333_3333;
    i0 = (i0 | (i0 << 1)) & 0x5555_5555;

    i1 = (i1 | (i1 << 8)) & 0x00FF_00FF;
    i1 = (i1 | (i1 << 4)) & 0x0F0F_0F0F;
    i1 = (i1 | (i1 << 2)) & 0x3333_3333;
    i1 = (i1 | (i1 << 1)) & 0x5555_5555;

    (i1 << 1) | i0
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{StreamExt, stream};
    use geo_traits::{CoordTrait, PointTrait};
    use geo_types::Point;
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::PointArray;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::{Dimension, PointType};
    use lance_core::ROW_ID;
    use lance_geo::bbox::total_bounds;
    use rand::Rng;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_hilbert_sort_same_x() {
        let point_type = PointType::new(Dimension::XY, Default::default());
        let schema = Arc::new(Schema::new(vec![
            point_type.to_field("bbox", true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let num_points = 100;
        let mut point_builder = PointBuilder::new(point_type.clone());
        let mut rng = rand::rng();
        for _ in 0..num_points {
            let y: f64 = rng.random_range(-180.0..180.0);
            point_builder.push_point(Some(&Point::new(33.3, y)));
        }

        let point_arr = point_builder.finish();
        let bbox = total_bounds(&point_arr).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                point_arr.into_array_ref(),
                Arc::new(UInt64Array::from_iter(0..num_points)),
            ],
        )
        .unwrap();

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(async move { Ok(batch) }),
        ));

        let sorter = HilbertSorter::new(bbox);
        let mut sorted = sorter.sort(stream).await.unwrap();

        let batch = sorted.next().await.unwrap().unwrap();
        let sorted_point_array =
            PointArray::try_from((batch.column(0).as_ref(), point_type)).unwrap();

        let mut prev = None;
        for item in sorted_point_array.iter() {
            let point = item.unwrap().unwrap();
            let current_y = point.coord().unwrap().y();
            if let Some(prev_y) = prev.take() {
                // Hilbert sort loses float precision during normalization.
                // So do an approximate check here to avoid flaky.
                assert!(current_y - prev_y > -0.0001);
                prev = Some(current_y);
            }
        }
    }
}
