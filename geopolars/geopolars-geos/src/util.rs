use geopolars_arrow::polygon::PolygonArray;
use geos::Geometry;
use polars::export::arrow::array::{BinaryArray, MutablePrimitiveArray, PrimitiveArray};

pub fn map_polygon_array_to_float_array<F>(arr: PolygonArray, op: F) -> PrimitiveArray<f64>
where
    F: Fn(Geometry) -> f64,
{
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(arr.len());

    let parts = arr.parts();
    for i in 0..parts.len() {
        let polygon = parts.get_as_geos(i);
        result.push(polygon.map(&op))
    }

    result.into()
}

pub fn map_wkb_array_to_float_array<F>(arr: &BinaryArray<i64>, op: F) -> PrimitiveArray<f64>
where
    F: Fn(Geometry) -> f64,
{
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(arr.len());

    for item in arr.iter() {
        let geom =
            item.map(|v| Geometry::new_from_wkb(v).expect("unable to convert to geos geometry"));
        result.push(geom.map(&op))
    }

    result.into()
}
