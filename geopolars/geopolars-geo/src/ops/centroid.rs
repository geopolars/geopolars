use crate::error::Result;
use geo::algorithm::centroid::Centroid;
use geoarrow::MutablePointArray;
use geoarrow::{GeometryArray, GeometryArrayTrait, PointArray};

pub(crate) fn centroid(array: GeometryArray) -> Result<PointArray> {
    let mut output_array = MutablePointArray::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArray::Point(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.map(|g| g.centroid())));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArray::MultiPoint(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push_geo(maybe_g.and_then(|g| g.centroid())));
        }
    }

    Ok(output_array.into())
}
