use arrow_schema::extension::ExtensionType;
use geoarrow_schema::{
    BoxType as GeoArrowBoxType, CoordType, Dimension, GeometryType as GeoArrowGeometryType,
    PointType as GeoArrowPointType, WkbType as GeoArrowWkbType, WktType as GeoArrowWktType,
};
use polars::prelude::DataType;
use polars::prelude::extension::{ExtensionTypeFactory, ExtensionTypeImpl};

use crate::factory::GeoArrowExtensionTypeFactory;
use crate::geoarrow::{BoxType, GeometryType, PointType, WkbType, WktType};

// --- constructor helpers ---------------------------------------------------

fn valid_point() -> PointType {
    PointType::new(GeoArrowPointType::new(Dimension::XY, Default::default()))
}

fn invalid_point(msg: &str) -> PointType {
    PointType::invalid(msg)
}

// --- TryFrom / roundtrip ---------------------------------------------------

#[test]
fn try_from_valid_roundtrips() {
    let inner = GeoArrowPointType::new(Dimension::XY, Default::default());
    let recovered = GeoArrowPointType::try_from(PointType::new(inner.clone())).unwrap();
    assert_eq!(inner, recovered);
}

#[test]
fn try_from_invalid_returns_err() {
    assert!(GeoArrowPointType::try_from(invalid_point("bad")).is_err());
}

#[test]
fn try_from_error_message_preserved() {
    let err = GeoArrowPointType::try_from(invalid_point("storage mismatch")).unwrap_err();
    assert!(err.contains("storage mismatch"), "got: {err}");
}

// --- From impl -------------------------------------------------------------

#[test]
fn from_inner_creates_valid_type() {
    let inner = GeoArrowPointType::new(Dimension::XY, Default::default());
    let pt = PointType::from(inner.clone());
    assert_eq!(GeoArrowPointType::try_from(pt).unwrap(), inner);
}

// --- name() ----------------------------------------------------------------

#[test]
fn name_is_correct_for_valid_type() {
    assert_eq!(valid_point().name(), geoarrow_schema::PointType::NAME);
}

#[test]
fn name_is_correct_for_invalid_type() {
    // name must always return the correct constant even when the inner value is an error
    assert_eq!(
        invalid_point("oops").name(),
        geoarrow_schema::PointType::NAME
    );
}

// --- serialize_metadata ----------------------------------------------------

#[test]
fn serialize_metadata_is_none_without_crs() {
    assert!(valid_point().serialize_metadata().is_none());
}

#[test]
fn serialize_metadata_is_none_for_invalid() {
    assert!(invalid_point("err").serialize_metadata().is_none());
}

// --- dyn_display / dyn_debug -----------------------------------------------

#[test]
fn dyn_display_valid_does_not_contain_invalid_marker() {
    let pt = valid_point();
    let s = pt.dyn_display();
    assert!(!s.contains("InvalidExtensionType"), "got: {s}");
}

#[test]
fn dyn_display_invalid_contains_error_message() {
    let pt = invalid_point("bad storage");
    let s = pt.dyn_display();
    assert!(s.contains("bad storage"), "got: {s}");
}

#[test]
fn dyn_debug_valid_does_not_contain_invalid_marker() {
    let pt = valid_point();
    let s = pt.dyn_debug();
    assert!(!s.contains("InvalidExtensionType"), "got: {s}");
}

#[test]
fn dyn_debug_invalid_contains_error_message() {
    let pt = invalid_point("bad storage");
    let s = pt.dyn_debug();
    assert!(s.contains("bad storage"), "got: {s}");
}

// --- Clone -----------------------------------------------------------------

#[test]
fn clone_valid_type_is_equal() {
    let pt = valid_point();
    assert_eq!(pt.clone(), pt);
}

#[test]
fn clone_invalid_type_is_still_invalid() {
    let pt = invalid_point("err");
    assert!(GeoArrowPointType::try_from(pt.clone()).is_err());
}

// --- PartialEq -------------------------------------------------------------

#[test]
fn equal_valid_types() {
    assert_eq!(valid_point(), valid_point());
}

#[test]
fn equal_invalid_types_same_message() {
    assert_eq!(invalid_point("e"), invalid_point("e"));
}

#[test]
fn unequal_invalid_types_different_messages() {
    assert_ne!(invalid_point("e1"), invalid_point("e2"));
}

#[test]
fn unequal_valid_and_invalid() {
    assert_ne!(valid_point(), invalid_point("err"));
}

// --- factory: happy path ---------------------------------------------------

#[test]
fn factory_valid_storage_produces_valid_type() {
    let factory = GeoArrowExtensionTypeFactory;
    // WKB storage is plain Binary, which needs no extra feature flags.
    let ext = factory.create_type_instance(geoarrow_schema::WkbType::NAME, &DataType::Binary, None);
    let display = ext.dyn_display();
    assert!(
        !display.contains("InvalidExtensionType"),
        "expected valid type, got: {display}"
    );
}

// --- factory: error path ---------------------------------------------------

#[test]
fn factory_invalid_storage_does_not_panic() {
    let factory = GeoArrowExtensionTypeFactory;
    let _ext =
        factory.create_type_instance(geoarrow_schema::PointType::NAME, &DataType::Int32, None);
}

#[test]
fn factory_invalid_storage_produces_invalid_state() {
    let factory = GeoArrowExtensionTypeFactory;
    let ext =
        factory.create_type_instance(geoarrow_schema::PointType::NAME, &DataType::Int32, None);
    let display = ext.dyn_display();
    assert!(
        display.contains("InvalidExtensionType"),
        "expected invalid state, got: {display}"
    );
}

#[test]
fn factory_invalid_storage_name_still_correct() {
    let factory = GeoArrowExtensionTypeFactory;
    let ext =
        factory.create_type_instance(geoarrow_schema::PointType::NAME, &DataType::Int32, None);
    assert_eq!(ext.name(), geoarrow_schema::PointType::NAME);
}

// --- dyn_display exact format -----------------------------------------------

#[test]
fn dyn_display_point_xy_separated() {
    // Default CoordType is Separated.
    let pt = PointType::new(GeoArrowPointType::new(Dimension::XY, Default::default()));
    assert_eq!(pt.dyn_display(), "point[xy, separated]");
}

#[test]
fn dyn_display_point_xyz_interleaved() {
    let inner = GeoArrowPointType::new(Dimension::XYZ, Default::default())
        .with_coord_type(CoordType::Interleaved);
    let pt = PointType::new(inner);
    assert_eq!(pt.dyn_display(), "point[xyz, interleaved]");
}

#[test]
fn dyn_display_geometry_interleaved() {
    let inner =
        GeoArrowGeometryType::new(Default::default()).with_coord_type(CoordType::Interleaved);
    let gt = GeometryType::new(inner);
    assert_eq!(gt.dyn_display(), "geometry[interleaved]");
}

#[test]
fn dyn_display_box_xyzm() {
    let inner = GeoArrowBoxType::new(Dimension::XYZM, Default::default());
    let bt = BoxType::new(inner);
    assert_eq!(bt.dyn_display(), "box[xyzm]");
}

#[test]
fn dyn_display_wkb() {
    let wkb = WkbType::new(GeoArrowWkbType::new(Default::default()));
    assert_eq!(wkb.dyn_display(), "wkb");
}

#[test]
fn dyn_display_wkt() {
    let wkt = WktType::new(GeoArrowWktType::new(Default::default()));
    assert_eq!(wkt.dyn_display(), "wkt");
}

// --- dyn_display vs dyn_debug for valid types --------------------------------

#[test]
fn dyn_display_differs_from_dyn_debug_for_valid_point() {
    let pt = valid_point();
    assert_ne!(
        pt.dyn_display(),
        pt.dyn_debug(),
        "display and debug should differ for valid types"
    );
}

#[test]
fn dyn_debug_valid_contains_struct_name() {
    // {:#?} / {:?} on geoarrow_schema types includes the type name.
    let pt = valid_point();
    let debug = pt.dyn_debug();
    assert!(debug.contains("PointType"), "got: {debug}");
}
