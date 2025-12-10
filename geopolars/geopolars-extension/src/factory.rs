use std::collections::HashMap;

use geoarrow_schema::GeoArrowType;
use geopolars_arrow::to_arrow::polars_field_to_arrow;
use polars::prelude::extension::{ExtensionTypeFactory, ExtensionTypeImpl};
use polars::prelude::{CompatLevel, DataType};

use crate::geoarrow::{
    BoxType, GeometryCollectionType, GeometryType, LineStringType, MultiLineStringType,
    MultiPointType, MultiPolygonType, PointType, PolygonType, WkbType, WktType,
};

/// A factory for creating GeoArrow extension types in Polars.
pub struct GeoArrowExtensionTypeFactory;

impl ExtensionTypeFactory for GeoArrowExtensionTypeFactory {
    fn create_type_instance(
        &self,
        name: &str,
        storage: &DataType,
        metadata: Option<&str>,
    ) -> Box<dyn ExtensionTypeImpl> {
        let arrow_data_type = polars_storage_type_to_arrow_data_type(storage);

        // Create Arrow field with extension metadata
        let mut arrow_metadata = HashMap::<String, String>::with_capacity(2);
        arrow_metadata.insert(
            arrow_schema::extension::EXTENSION_TYPE_NAME_KEY.to_string(),
            name.to_string(),
        );
        if let Some(metadata) = metadata {
            arrow_metadata.insert(
                arrow_schema::extension::EXTENSION_TYPE_METADATA_KEY.to_string(),
                metadata.to_string(),
            );
        }

        let arrow_field =
            arrow_schema::Field::new("", arrow_data_type, true).with_metadata(arrow_metadata);

        // TODO: are we assured that the name matches the type here?
        // What do we do if the storage type isn't compatible with the extension type?
        let geoarrow_type = GeoArrowType::from_extension_field(&arrow_field)
            .expect("Creation of GeoArrow extension type");

        match geoarrow_type {
            GeoArrowType::Point(t) => Box::new(PointType::new(t)),
            GeoArrowType::LineString(t) => Box::new(LineStringType::new(t)),
            GeoArrowType::Polygon(t) => Box::new(PolygonType::new(t)),
            GeoArrowType::MultiPoint(t) => Box::new(MultiPointType::new(t)),
            GeoArrowType::MultiLineString(t) => Box::new(MultiLineStringType::new(t)),
            GeoArrowType::MultiPolygon(t) => Box::new(MultiPolygonType::new(t)),
            GeoArrowType::GeometryCollection(t) => Box::new(GeometryCollectionType::new(t)),
            GeoArrowType::Geometry(t) => Box::new(GeometryType::new(t)),
            GeoArrowType::Rect(t) => Box::new(BoxType::new(t)),
            GeoArrowType::Wkb(t) => Box::new(WkbType::new(t)),
            GeoArrowType::LargeWkb(t) => Box::new(WkbType::new(t)),
            GeoArrowType::WkbView(t) => Box::new(WkbType::new(t)),
            GeoArrowType::Wkt(t) => Box::new(WktType::new(t)),
            GeoArrowType::LargeWkt(t) => Box::new(WktType::new(t)),
            GeoArrowType::WktView(t) => Box::new(WktType::new(t)),
        }
    }
}

fn polars_storage_type_to_arrow_data_type(storage: &DataType) -> arrow_schema::DataType {
    let polars_field = polars_arrow::datatypes::Field::new(
        "".into(),
        storage.to_arrow(CompatLevel::newest()),
        true,
    );
    let arrow_field_from_ffi = polars_field_to_arrow(&polars_field);
    arrow_field_from_ffi.data_type().clone()
}
