use core::any::type_name;
use polars::error::PolarsError;
use thiserror::Error;

#[cfg(feature = "proj")]
use proj::{ProjCreateError, ProjError};

#[derive(Error, Debug)]
pub enum GeopolarsError {
    // Copied from geo-types:
    // https://github.com/georust/geo/blob/a1226940a674c7ac5d1db43d495520e418af8907/geo-types/src/error.rs
    #[error("Expected {expected} (found {found})")]
    MismatchedGeometry {
        expected: &'static str,
        found: &'static str,
    },

    #[cfg(feature = "proj")]
    #[error(transparent)]
    ProjCreateError(Box<ProjCreateError>),

    #[cfg(feature = "proj")]
    #[error(transparent)]
    ProjError(Box<ProjError>),

    #[error(transparent)]
    PolarsError(Box<PolarsError>),
}

pub type Result<T> = std::result::Result<T, GeopolarsError>;

impl From<PolarsError> for GeopolarsError {
    fn from(err: PolarsError) -> Self {
        Self::PolarsError(Box::new(err))
    }
}

#[cfg(feature = "proj")]
impl From<ProjCreateError> for GeopolarsError {
    fn from(err: ProjCreateError) -> Self {
        Self::ProjCreateError(Box::new(err))
    }
}

#[cfg(feature = "proj")]
impl From<ProjError> for GeopolarsError {
    fn from(err: ProjError) -> Self {
        Self::ProjError(Box::new(err))
    }
}

/// Helper to go from geometry object to string name of geometry type
/// Copied from
/// https://github.com/georust/geo/blob/a1226940a674c7ac5d1db43d495520e418af8907/geo-types/src/geometry/mod.rs#L253-L269
pub(crate) fn inner_type_name<T>(geometry: &geo::Geometry<T>) -> &'static str
where
    T: geo::CoordNum,
{
    match geometry {
        geo::Geometry::Point(_) => type_name::<geo::Point<T>>(),
        geo::Geometry::Line(_) => type_name::<geo::Line<T>>(),
        geo::Geometry::LineString(_) => type_name::<geo::LineString<T>>(),
        geo::Geometry::Polygon(_) => type_name::<geo::Polygon<T>>(),
        geo::Geometry::MultiPoint(_) => type_name::<geo::MultiPoint<T>>(),
        geo::Geometry::MultiLineString(_) => type_name::<geo::MultiLineString<T>>(),
        geo::Geometry::MultiPolygon(_) => type_name::<geo::MultiPolygon<T>>(),
        geo::Geometry::GeometryCollection(_) => type_name::<geo::GeometryCollection<T>>(),
        geo::Geometry::Rect(_) => type_name::<geo::Rect<T>>(),
        geo::Geometry::Triangle(_) => type_name::<geo::Triangle<T>>(),
    }
}
