use geopolars_arrow::polygon::PolygonArray;
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

#[cfg(test)]
mod tests {
    use super::area;
    use approx::assert_relative_eq;
    use geo::{polygon, Polygon};
    use geopolars_arrow::polygon::MutablePolygonArray;
    use polars::export::arrow::array::PrimitiveArray;

    fn call_area(input: Vec<Polygon>) -> PrimitiveArray<f64> {
        let mut_polygon_arr: MutablePolygonArray = input.into();
        let polygon_arr = mut_polygon_arr.into_arrow();

        let result = area(&polygon_arr);
        let result_arr = result
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        result_arr.clone()
    }

    #[test]
    fn area_empty_polygon_test() {
        let polygons = vec![polygon![]];
        let result = call_area(polygons);
        assert_eq!(result.value(0), 0.0_f64);
    }

    #[test]
    fn area_polygon_test() {
        let polygons = vec![polygon![
            (x: 0., y: 0.),
            (x: 5., y: 0.),
            (x: 5., y: 6.),
            (x: 0., y: 6.),
            (x: 0., y: 0.)
        ]];
        let result = call_area(polygons);
        assert_relative_eq!(result.value(0), 30.);
    }
}
