use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, UInt64Array};
use arrow_schema::Field;
use geoarrow_array::cast::{AsGeoArrowArray, to_wkb};
use geoarrow_schema::{CoordType, Dimension};

use crate::to_arrow::polars_to_arrow;
use crate::to_geoarrow::polars_to_geoarrow;
use crate::to_polars::{arrow_to_polars, geoarrow_to_polars};

#[test]
fn round_trip_integer() {
    let array = UInt64Array::from(vec![1, 2, 3, 4]);
    // Field name not preserved in round trip
    let field = Field::new("", array.data_type().clone(), true);
    let polars_array = arrow_to_polars(&array, &field);
    let (back_array, back_field) = polars_to_arrow(polars_array);

    assert_eq!(&array, back_array.as_primitive::<UInt64Type>());
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
            let array = geoarrow_array::test::point::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_point());
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
            let array = geoarrow_array::test::linestring::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_line_string());
        }
    }
}

#[test]
fn round_trip_polygon() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = geoarrow_array::test::polygon::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_polygon());
        }
    }
}

#[test]
fn round_trip_multi_point() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = geoarrow_array::test::multipoint::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_multi_point());
        }
    }
}

#[test]
fn round_trip_multi_line_string() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = geoarrow_array::test::multilinestring::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_multi_line_string());
        }
    }
}

#[test]
fn round_trip_multi_polygon() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = geoarrow_array::test::multipolygon::array(coord_type, dim);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_multi_polygon());
        }
    }
}

#[test]
fn round_trip_geometry_collection() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            let array = geoarrow_array::test::geometrycollection::array(coord_type, dim, false);
            let polars_array = geoarrow_to_polars(&array);
            let back = polars_to_geoarrow(polars_array);
            assert_eq!(&array, back.as_geometry_collection());
        }
    }
}

#[test]
fn round_trip_geometry() {
    for coord_type in [CoordType::Interleaved, CoordType::Separated] {
        let array = geoarrow_array::test::geometry::array(coord_type, false);
        let polars_array = geoarrow_to_polars(&array);
        let back = polars_to_geoarrow(polars_array);
        assert_eq!(&array, back.as_geometry());
    }
}

#[test]
fn round_trip_wkb() {
    let array = geoarrow_array::test::geometry::array(CoordType::default(), false);
    let wkb_array = to_wkb::<i32>(&array).unwrap();
    let polars_array = geoarrow_to_polars(&wkb_array);
    let back = polars_to_geoarrow(polars_array);
    assert_eq!(&wkb_array, back.as_wkb::<i32>());
}
