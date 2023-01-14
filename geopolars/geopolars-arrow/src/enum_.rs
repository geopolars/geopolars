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
}
