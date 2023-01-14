use crate::error::Result;
use geo::prelude::Area;
use geopolars_arrow::GeometryArrayEnum;
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};

pub(crate) fn area(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
    }

    Ok(output_array.into())
}
