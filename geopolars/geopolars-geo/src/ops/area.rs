use crate::error::Result;
use geo::prelude::Area;
use geoarrow::{GeometryArray, GeometryArrayTrait};
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};

pub(crate) fn area(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
    }

    Ok(output_array.into())
}
