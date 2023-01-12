use geopolars_arrow::polygon::array::PolygonArray;
use geopolars_arrow::util::{get_geoarrow_array_type, GeoArrowType};
use geos::Geom;
use polars::export::arrow::array::{Array, BinaryArray, ListArray};

use crate::util::{map_polygon_array_to_float_array, map_wkb_array_to_float_array};

pub fn area(arr: &dyn Array) -> Box<dyn Array> {
    match get_geoarrow_array_type(arr) {
        GeoArrowType::WKB => {
            let binary_arr = arr.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
            let result_arr = map_wkb_array_to_float_array(binary_arr, |g| g.area().unwrap());
            Box::new(result_arr) as Box<dyn Array>
        }
        GeoArrowType::Polygon => {
            let polygon_arr = PolygonArray(arr.as_any().downcast_ref::<ListArray<i64>>().unwrap());
            let result_arr = map_polygon_array_to_float_array(polygon_arr, |g| g.area().unwrap());
            Box::new(result_arr) as Box<dyn Array>
        }
        _ => panic!("Unexpected geometry type for operation: area"),
    }
}
