use std::sync::Arc;

use arrow_schema::Field;
use geoarrow_schema::{CoordType, Crs, Dimension, GeoArrowType, Metadata, PointType, WkbType};

use crate::to_arrow::polars_field_to_arrow;
use crate::to_geoarrow::polars_field_to_geoarrow;
use crate::to_polars::{arrow_field_to_polars, geoarrow_type_to_polars};

#[test]
fn round_trip_integer() {
    // Field name not preserved in round trip
    let field = Field::new("", arrow_schema::DataType::UInt64, true);
    let polars_field = arrow_field_to_polars(&field);
    let back_field = polars_field_to_arrow(&polars_field);

    assert_eq!(&field, back_field.as_ref());
}

#[test]
fn round_trip_point() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let geoarrow_type = GeoArrowType::from(
                PointType::new(dim, Default::default()).with_coord_type(coord_type),
            );
            let polars_field = geoarrow_type_to_polars(&geoarrow_type);
            let back_type = polars_field_to_geoarrow(&polars_field);

            assert_eq!(geoarrow_type, back_type);
        }
    }
}

#[test]
fn round_trip_point_with_crs() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));

    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let geoarrow_type = GeoArrowType::from(
                PointType::new(dim, metadata.clone()).with_coord_type(coord_type),
            );
            let polars_field = geoarrow_type_to_polars(&geoarrow_type);
            let back_type = polars_field_to_geoarrow(&polars_field);

            assert_eq!(geoarrow_type, back_type);
        }
    }
}

#[test]
fn round_trip_line_string() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let geoarrow_type = GeoArrowType::from(
                geoarrow_schema::LineStringType::new(dim, Default::default())
                    .with_coord_type(coord_type),
            );
            let polars_field = geoarrow_type_to_polars(&geoarrow_type);
            let back_type = polars_field_to_geoarrow(&polars_field);
            assert_eq!(geoarrow_type, back_type);
        }
    }
}

#[test]
fn round_trip_wkb() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));

    let geoarrow_type = GeoArrowType::Wkb(WkbType::new(metadata.clone()));
    let polars_field = geoarrow_type_to_polars(&geoarrow_type);
    let back_type = polars_field_to_geoarrow(&polars_field);
    assert_eq!(geoarrow_type, back_type);
}

#[test]
fn round_trip_large_wkb() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));

    let geoarrow_type = GeoArrowType::LargeWkb(WkbType::new(metadata.clone()));
    let polars_field = geoarrow_type_to_polars(&geoarrow_type);
    let back_type = polars_field_to_geoarrow(&polars_field);
    assert_eq!(geoarrow_type, back_type);
}

#[test]
fn round_trip_wkb_view() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    let metadata = Arc::new(Metadata::new(crs, None));

    let geoarrow_type = GeoArrowType::WkbView(WkbType::new(metadata.clone()));
    let polars_field = geoarrow_type_to_polars(&geoarrow_type);
    let back_type = polars_field_to_geoarrow(&polars_field);
    assert_eq!(geoarrow_type, back_type);
}
