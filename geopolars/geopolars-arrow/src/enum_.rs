use polars::export::arrow::array::Array;

use crate::{
    LineStringArray, MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray,
    PolygonArray, WKBArray,
};

#[derive(Clone, Debug)]
pub enum GeometryType {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    WKB,
}

pub enum GeometryArrayEnum {
    Point(PointArray),
    LineString(LineStringArray),
    Polygon(PolygonArray),
    MultiPoint(MultiPointArray),
    MultiLineString(MultiLineStringArray),
    MultiPolygon(MultiPolygonArray),
    WKB(WKBArray),
}

impl GeometryArrayEnum {
    /// Returns the number of geometries in this array
    pub fn len(&self) -> usize {
        match self {
            GeometryArrayEnum::Point(arr) => arr.len(),
            GeometryArrayEnum::LineString(arr) => arr.len(),
            GeometryArrayEnum::Polygon(arr) => arr.len(),
            GeometryArrayEnum::MultiPoint(arr) => arr.len(),
            GeometryArrayEnum::MultiLineString(arr) => arr.len(),
            GeometryArrayEnum::MultiPolygon(arr) => arr.len(),
            GeometryArrayEnum::WKB(arr) => arr.len(),
        }
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_arrow(self) -> Box<dyn Array> {
        match self {
            GeometryArrayEnum::Point(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::LineString(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::Polygon(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::MultiPoint(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::MultiLineString(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::MultiPolygon(arr) => arr.into_arrow().boxed(),
            GeometryArrayEnum::WKB(arr) => arr.into_arrow().boxed(),
        }
    }
}
