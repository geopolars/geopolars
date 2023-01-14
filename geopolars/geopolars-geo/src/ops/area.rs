use crate::error::Result;
use crate::util::{iter_geom, map_polygon_series_to_float_series};
use geo::prelude::Area;

use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use geopolars_arrow::{
    GeometryArray, GeometryType, LineStringArray, MultiLineStringArray, MultiPointArray,
    MultiPolygonArray, PointArray, PolygonArray, WKBArray,
};
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray, StructArray};
use polars::prelude::Series;

pub(crate) fn area(array: &GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array.geometry_type() {
        GeometryType::WKB => {
            let arr = array.as_any().downcast_ref::<WKBArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::Point => {
            let arr = array.as_any().downcast_ref::<PointArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::LineString => {
            let arr = array.as_any().downcast_ref::<LineStringArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::Polygon => {
            let arr = array.as_any().downcast_ref::<PolygonArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::MultiPoint => {
            let arr = array.as_any().downcast_ref::<MultiPointArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::MultiLineString => {
            let arr = array
                .as_any()
                .downcast_ref::<MultiLineStringArray>()
                .unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
        GeometryType::MultiPolygon => {
            let arr = array.as_any().downcast_ref::<MultiPolygonArray>().unwrap();
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.unsigned_area())));
        }
    }
}
