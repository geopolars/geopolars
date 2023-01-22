use arrow2::array::BinaryArray;
use geo::BoundingRect;
use geozero::ToGeo;
use rstar::{RTreeObject, AABB};

/// An arrow equivalent of a Point
#[derive(Debug, Clone)]
pub struct WKB<'a> {
    pub arr: &'a BinaryArray<i64>,
    pub geom_index: usize,
}

impl From<WKB<'_>> for geo::Geometry {
    fn from(value: WKB<'_>) -> Self {
        (&value).into()
    }
}

impl From<&WKB<'_>> for geo::Geometry {
    fn from(value: &WKB<'_>) -> Self {
        let buf = value.arr.value(value.geom_index);
        geozero::wkb::Wkb(buf.to_vec()).to_geo().unwrap()
    }
}

impl RTreeObject for WKB<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let geom: geo::Geometry = self.into();
        let rect = geom.bounding_rect().unwrap();
        let lower: [f64; 2] = rect.min().into();
        let upper: [f64; 2] = rect.max().into();
        AABB::from_corners(lower, upper)
    }
}
