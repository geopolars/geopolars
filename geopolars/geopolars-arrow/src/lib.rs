//! Helpers for working with GeoArrow geometries
//!
//! At some point in the future, this will likely become a public geoarrow module, or be integrated
//! into geozero

pub use binary::{MutableWKBArray, WKBArray};
pub use enum_::{GeometryArrayEnum, GeometryType};
pub use linestring::LineStringArray;
pub use multilinestring::MultiLineStringArray;
pub use multipoint::MultiPointArray;
pub use multipolygon::MultiPolygonArray;
pub use point::{MutablePointArray, PointArray};
pub use polygon::PolygonArray;
pub use trait_::GeometryArray;

pub mod binary;
pub mod enum_;
pub mod error;
pub mod linestring;
mod multilinestring;
mod multipoint;
mod multipolygon;
pub mod point;
pub mod polygon;
pub mod trait_;
pub mod util;
