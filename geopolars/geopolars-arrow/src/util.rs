use crate::GeometryArrayEnum;
use polars::datatypes::DataType;
use polars::export::arrow::array::{Array, BinaryArray, ListArray, StructArray};
use polars::prelude::{ArrowDataType, Series};

pub enum GeoArrowType {
    Point,
    LineString,
    Polygon,
    WKB,
}

pub fn array_to_geometry_array(arr: &dyn Array, is_multi: bool) -> GeometryArrayEnum {
    match arr.data_type() {
        ArrowDataType::LargeBinary => {
            let lit_arr = arr.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            GeometryArrayEnum::WKB(lit_arr.clone().into())
        }
        ArrowDataType::Struct(_) => {
            let lit_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
            GeometryArrayEnum::Point(lit_arr.clone().try_into().unwrap())
        }
        ArrowDataType::List(dt) | ArrowDataType::LargeList(dt) => match dt.data_type() {
            ArrowDataType::Struct(_) => {
                let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();

                if is_multi {
                    GeometryArrayEnum::MultiPoint(lit_arr.clone().try_into().unwrap())
                } else {
                    GeometryArrayEnum::LineString(lit_arr.clone().try_into().unwrap())
                }
            }
            ArrowDataType::List(dt2) | ArrowDataType::LargeList(dt2) => match dt2.data_type() {
                ArrowDataType::Struct(_) => {
                    let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                    if is_multi {
                        GeometryArrayEnum::MultiLineString(lit_arr.clone().try_into().unwrap())
                    } else {
                        GeometryArrayEnum::Polygon(lit_arr.clone().try_into().unwrap())
                    }
                }
                ArrowDataType::List(_) | ArrowDataType::LargeList(_) => {
                    let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                    GeometryArrayEnum::MultiPolygon(lit_arr.clone().try_into().unwrap())
                }
                _ => panic!("Unexpected inner list type: {:?}", dt2),
            },
            _ => panic!("Unexpected inner list type: {:?}", dt),
        },
        dt => panic!("Unexpected geoarrow type: {:?}", dt),
    }
}

pub fn get_geoarrow_array_type(arr: &dyn Array) -> GeoArrowType {
    match arr.data_type() {
        ArrowDataType::Binary => GeoArrowType::WKB,
        ArrowDataType::Struct(_) => GeoArrowType::Point,
        ArrowDataType::List(dt) | ArrowDataType::LargeList(dt) => match dt.data_type() {
            ArrowDataType::Struct(_) => GeoArrowType::LineString,
            ArrowDataType::List(_) | ArrowDataType::LargeList(_) => GeoArrowType::Polygon,
            _ => panic!("Unexpected inner list type: {:?}", dt),
        },
        dt => panic!("Unexpected geoarrow type: {:?}", dt),
    }
}

pub fn get_geoarrow_type(series: &Series) -> GeoArrowType {
    match series.dtype() {
        DataType::Binary => GeoArrowType::WKB,
        DataType::Struct(_) => GeoArrowType::Point,
        DataType::List(dt) => match *dt.clone() {
            DataType::Struct(_) => GeoArrowType::LineString,
            DataType::List(_) => GeoArrowType::Polygon,
            _ => panic!("Unexpected inner list type: {}", dt),
        },

        dt => panic!("Unexpected geoarrow type: {}", dt),
    }
}
