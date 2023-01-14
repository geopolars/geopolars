use crate::error::Result;
use geo::algorithm::centroid::Centroid;
use geopolars_arrow::MutablePointArray;
use geopolars_arrow::{GeometryArrayEnum, PointArray};

pub(crate) fn centroid(array: GeometryArrayEnum) -> Result<PointArray> {
    let mut output_array = MutablePointArray::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArrayEnum::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.map(|g| g.centroid())));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
    }

    Ok(output_array.into())
}
