use crate::GeometryArrayEnum;
use arrow2::array::{Array, BinaryArray, ListArray, StructArray};
use arrow2::datatypes::DataType;

pub fn array_to_geometry_array(arr: &dyn Array, is_multi: bool) -> GeometryArrayEnum {
    match arr.data_type() {
        DataType::LargeBinary => {
            let lit_arr = arr.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            GeometryArrayEnum::WKB(lit_arr.clone().into())
        }
        DataType::Struct(_) => {
            let lit_arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
            GeometryArrayEnum::Point(lit_arr.clone().try_into().unwrap())
        }
        DataType::List(dt) | DataType::LargeList(dt) => match dt.data_type() {
            DataType::Struct(_) => {
                let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();

                if is_multi {
                    GeometryArrayEnum::MultiPoint(lit_arr.clone().try_into().unwrap())
                } else {
                    GeometryArrayEnum::LineString(lit_arr.clone().try_into().unwrap())
                }
            }
            DataType::List(dt2) | DataType::LargeList(dt2) => match dt2.data_type() {
                DataType::Struct(_) => {
                    let lit_arr = arr.as_any().downcast_ref::<ListArray<i64>>().unwrap();
                    if is_multi {
                        GeometryArrayEnum::MultiLineString(lit_arr.clone().try_into().unwrap())
                    } else {
                        GeometryArrayEnum::Polygon(lit_arr.clone().try_into().unwrap())
                    }
                }
                DataType::List(_) | DataType::LargeList(_) => {
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
