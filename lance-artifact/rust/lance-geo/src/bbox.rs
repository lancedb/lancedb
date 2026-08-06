// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, GeometryType, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait, UnimplementedGeometryCollection, UnimplementedLine, UnimplementedLineString,
    UnimplementedMultiLineString, UnimplementedMultiPoint, UnimplementedMultiPolygon,
    UnimplementedPoint, UnimplementedPolygon, UnimplementedTriangle,
};
use geo_types::Coord;
use geoarrow_array::array::RectArray;
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{BoxType, Dimension};
use lance_core::error::ArrowResult;
use serde::{Deserialize, Serialize};

/// Inspired by <https://github.com/geoarrow/geoarrow-rs>
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BoundingBox {
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
}

impl BoundingBox {
    pub fn new() -> Self {
        Self {
            minx: f64::INFINITY,
            miny: f64::INFINITY,
            maxx: -f64::INFINITY,
            maxy: -f64::INFINITY,
        }
    }

    pub fn new_with_coords(coords: &[impl CoordTrait<T = f64>]) -> Self {
        let mut new_rect = Self::new();
        for coord in coords {
            new_rect.add_coord(coord);
        }
        new_rect
    }

    pub fn new_with_rect(rect: &impl RectTrait<T = f64>) -> Self {
        let mut new_rect = Self::new();
        new_rect.add_rect(rect);
        new_rect
    }

    pub fn minx(&self) -> f64 {
        self.minx
    }

    pub fn miny(&self) -> f64 {
        self.miny
    }

    pub fn maxx(&self) -> f64 {
        self.maxx
    }

    pub fn maxy(&self) -> f64 {
        self.maxy
    }

    pub fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        let x = coord.x();
        let y = coord.y();

        if x < self.minx {
            self.minx = x;
        }
        if y < self.miny {
            self.miny = y;
        }

        if x > self.maxx {
            self.maxx = x;
        }
        if y > self.maxy {
            self.maxy = y;
        }
    }

    pub fn add_point(&mut self, point: &impl PointTrait<T = f64>) {
        if let Some(coord) = point.coord() {
            self.add_coord(&coord);
        }
    }

    pub fn add_line_string(&mut self, line_string: &impl LineStringTrait<T = f64>) {
        for coord in line_string.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_rect(&mut self, rect: &impl RectTrait<T = f64>) {
        let min = rect.min();
        let max = rect.max();

        // Empty bounding boxes use inverted bounds, so they are the identity for a union.
        if min.x() > max.x() || min.y() > max.y() {
            return;
        }

        self.add_coord(&min);
        self.add_coord(&max);
    }

    pub fn add_polygon(&mut self, polygon: &impl PolygonTrait<T = f64>) {
        if let Some(exterior_ring) = polygon.exterior() {
            self.add_line_string(&exterior_ring);
        }

        for exterior in polygon.interiors() {
            self.add_line_string(&exterior)
        }
    }

    pub fn add_multi_point(&mut self, multi_point: &impl MultiPointTrait<T = f64>) {
        for point in multi_point.points() {
            self.add_point(&point);
        }
    }

    pub fn add_multi_line_string(
        &mut self,
        multi_line_string: &impl MultiLineStringTrait<T = f64>,
    ) {
        for linestring in multi_line_string.line_strings() {
            self.add_line_string(&linestring);
        }
    }

    pub fn add_multi_polygon(&mut self, multi_polygon: &impl MultiPolygonTrait<T = f64>) {
        for polygon in multi_polygon.polygons() {
            self.add_polygon(&polygon);
        }
    }

    pub fn add_triangle(&mut self, triangle: &impl TriangleTrait<T = f64>) {
        for coord in triangle.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_line(&mut self, line: &impl LineTrait<T = f64>) {
        for coord in line.coords() {
            self.add_coord(&coord);
        }
    }

    pub fn add_geometry(&mut self, geometry: &impl GeometryTrait<T = f64>) {
        use geo_traits::GeometryType::{
            GeometryCollection, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Point,
            Polygon, Rect, Triangle,
        };

        match geometry.as_type() {
            Point(g) => self.add_point(g),
            LineString(g) => self.add_line_string(g),
            Polygon(g) => self.add_polygon(g),
            MultiPoint(g) => self.add_multi_point(g),
            MultiLineString(g) => self.add_multi_line_string(g),
            MultiPolygon(g) => self.add_multi_polygon(g),
            GeometryCollection(g) => self.add_geometry_collection(g),
            Rect(g) => self.add_rect(g),
            Triangle(g) => self.add_triangle(g),
            Line(g) => self.add_line(g),
        }
    }

    pub fn add_geometry_collection(
        &mut self,
        geometry_collection: &impl GeometryCollectionTrait<T = f64>,
    ) {
        for geometry in geometry_collection.geometries() {
            self.add_geometry(&geometry);
        }
    }

    pub fn add_geo_arrow_array(&mut self, arr: &dyn GeoArrowArray) -> ArrowResult<()> {
        let bbox = total_bounds(arr)?;
        self.add_geometry(&bbox);

        Ok(())
    }

    pub fn rect_intersects(&self, other: &impl RectTrait<T = f64>) -> bool {
        if self.maxx() < other.min().x() {
            return false;
        }

        if self.maxy() < other.min().y() {
            return false;
        }

        if self.minx() > other.max().x() {
            return false;
        }

        if self.miny() > other.max().y() {
            return false;
        }

        true
    }
}

impl Default for BoundingBox {
    fn default() -> Self {
        Self::new()
    }
}

impl RectTrait for BoundingBox {
    type CoordType<'a> = Coord;

    fn min(&self) -> Self::CoordType<'_> {
        Coord {
            x: self.minx,
            y: self.miny,
        }
    }

    fn max(&self) -> Self::CoordType<'_> {
        Coord {
            x: self.maxx,
            y: self.maxy,
        }
    }
}

impl GeometryTrait for BoundingBox {
    type T = f64;
    type PointType<'a>
        = UnimplementedPoint<f64>
    where
        Self: 'a;
    type LineStringType<'a>
        = UnimplementedLineString<f64>
    where
        Self: 'a;
    type PolygonType<'a>
        = UnimplementedPolygon<f64>
    where
        Self: 'a;
    type MultiPointType<'a>
        = UnimplementedMultiPoint<f64>
    where
        Self: 'a;
    type MultiLineStringType<'a>
        = UnimplementedMultiLineString<f64>
    where
        Self: 'a;
    type MultiPolygonType<'a>
        = UnimplementedMultiPolygon<f64>
    where
        Self: 'a;
    type GeometryCollectionType<'a>
        = UnimplementedGeometryCollection<f64>
    where
        Self: 'a;
    type RectType<'a>
        = Self
    where
        Self: 'a;
    type TriangleType<'a>
        = UnimplementedTriangle<f64>
    where
        Self: 'a;
    type LineType<'a>
        = UnimplementedLine<f64>
    where
        Self: 'a;

    fn dim(&self) -> geo_traits::Dimensions {
        geo_traits::Dimensions::Xy
    }

    fn as_type(
        &self,
    ) -> GeometryType<
        '_,
        Self::PointType<'_>,
        Self::LineStringType<'_>,
        Self::PolygonType<'_>,
        Self::MultiPointType<'_>,
        Self::MultiLineStringType<'_>,
        Self::MultiPolygonType<'_>,
        Self::GeometryCollectionType<'_>,
        Self::RectType<'_>,
        Self::TriangleType<'_>,
        Self::LineType<'_>,
    > {
        GeometryType::Rect(self)
    }
}

/// Create a new RectArray using the bounding box of each geometry.
///
/// Note that this **does not** currently correctly handle the antimeridian
pub fn bounding_box(arr: &dyn GeoArrowArray) -> ArrowResult<RectArray> {
    downcast_geoarrow_array!(arr, impl_array_accessor)
}

/// The actual implementation of computing the bounding box
fn impl_array_accessor<'a>(arr: &'a impl GeoArrowArrayAccessor<'a>) -> ArrowResult<RectArray> {
    let mut builder = RectBuilder::with_capacity(
        BoxType::new(Dimension::XY, arr.data_type().metadata().clone()),
        arr.len(),
    );
    for item in arr.iter() {
        if let Some(item) = item {
            let mut bbox = BoundingBox::new();
            bbox.add_geometry(&item?);
            builder.push_rect(Some(&bbox));
        } else {
            builder.push_null();
        }
    }
    Ok(builder.finish())
}

/// Get the total bounds (i.e. minx, miny, maxx, maxy) of the entire geoarrow array.
pub fn total_bounds(arr: &dyn GeoArrowArray) -> ArrowResult<BoundingBox> {
    downcast_geoarrow_array!(arr, impl_total_bounds)
}

/// The actual implementation of computing the total bounds
fn impl_total_bounds<'a>(arr: &'a impl GeoArrowArrayAccessor<'a>) -> ArrowResult<BoundingBox> {
    let mut bbox = BoundingBox::new();

    for item in arr.iter().flatten() {
        bbox.add_geometry(&item?);
    }

    Ok(bbox)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_empty_rect_is_noop() {
        let valid = BoundingBox::new_with_coords(&[
            geo_types::coord! { x: -1.0, y: -2.0 },
            geo_types::coord! { x: 3.0, y: 4.0 },
        ]);
        let empty = BoundingBox::new();

        let mut valid_then_empty = valid;
        valid_then_empty.add_rect(&empty);

        let mut empty_then_valid = empty;
        empty_then_valid.add_rect(&valid);

        for bbox in [valid_then_empty, empty_then_valid] {
            assert_eq!(bbox.minx(), -1.0);
            assert_eq!(bbox.miny(), -2.0);
            assert_eq!(bbox.maxx(), 3.0);
            assert_eq!(bbox.maxy(), 4.0);
        }
    }
}
