use crate::error::Result;
use geo::dimensions::HasDimensions;
use geopolars_arrow::GeometryArrayEnum;
use polars::export::arrow::array::{BooleanArray, MutableBooleanArray};

pub(crate) fn is_empty(array: GeometryArrayEnum) -> Result<BooleanArray> {
    let mut output_array = MutableBooleanArray::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.is_empty())));
        }
    }

    Ok(output_array.into())
}
