//! Helpers for using Polygon GeoArrow data

pub(crate) use array::parse_polygon;
pub use array::PolygonArray;
pub use mutable::MutablePolygonArray;
pub use scalar::Polygon;

mod array;
mod mutable;
mod scalar;
