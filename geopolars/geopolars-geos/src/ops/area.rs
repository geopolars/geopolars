use geoarrow::{GeometryArray, GeometryArrayTrait};
use geos::Geom;
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};

pub fn area(array: GeometryArray) -> PrimitiveArray<f64> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geos()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geos()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geos()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geos()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        }
        // GeometryArray::MultiPoint(arr) => {
        //     arr.iter_geos()
        //         .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        // }
        // GeometryArray::MultiLineString(arr) => {
        //     arr.iter_geos()
        //         .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        // }
        // GeometryArray::MultiPolygon(arr) => {
        //     arr.iter_geos()
        //         .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.area().unwrap())));
        // }
        _ => unimplemented!(),
    }

    output_array.into()
}

#[cfg(test)]
mod tests {
    use super::area;
    use approx::assert_relative_eq;
    use geo::{polygon, Polygon};
    use geoarrow::polygon::MutablePolygonArray;
    use geoarrow::{GeometryArray, PolygonArray};
    use polars::export::arrow::array::{Array, PrimitiveArray};

    fn call_area(input: Vec<Polygon>) -> PrimitiveArray<f64> {
        let mut_polygon_arr: MutablePolygonArray = input.into();
        let polygon_arr = mut_polygon_arr.into_arrow();

        let polygon_arr2: PolygonArray = polygon_arr.try_into().unwrap();

        let result = area(GeometryArray::Polygon(polygon_arr2));
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
