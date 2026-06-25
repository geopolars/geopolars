pub mod factory;
pub mod geoarrow;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_schema::extension::ExtensionType;
use polars::error::PolarsResult;
use polars_core::datatypes::extension::register_extension_type;

use crate::factory::GeoArrowExtensionTypeFactory;

/// Register all GeoArrow extension types into the Polars extension registry.
pub fn register_all_extensions() -> PolarsResult<()> {
    let factory = Arc::new(GeoArrowExtensionTypeFactory);

    register_extension_type(geoarrow_schema::PointType::NAME, Some(factory.clone()))?;
    register_extension_type(geoarrow_schema::LineStringType::NAME, Some(factory.clone()))?;
    register_extension_type(geoarrow_schema::PolygonType::NAME, Some(factory.clone()))?;
    register_extension_type(geoarrow_schema::MultiPointType::NAME, Some(factory.clone()))?;
    register_extension_type(
        geoarrow_schema::MultiLineStringType::NAME,
        Some(factory.clone()),
    )?;
    register_extension_type(
        geoarrow_schema::MultiPolygonType::NAME,
        Some(factory.clone()),
    )?;
    register_extension_type(
        geoarrow_schema::GeometryCollectionType::NAME,
        Some(factory.clone()),
    )?;
    register_extension_type(geoarrow_schema::GeometryType::NAME, Some(factory.clone()))?;
    register_extension_type(geoarrow_schema::BoxType::NAME, Some(factory.clone()))?;

    register_extension_type(geoarrow_schema::WkbType::NAME, Some(factory.clone()))?;
    register_extension_type(geoarrow_schema::WktType::NAME, Some(factory))?;

    Ok(())
}
