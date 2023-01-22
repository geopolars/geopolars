use crate::error::Result;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::Polygon;
use geopolars_arrow::GeometryArray;

pub(crate) fn envelope(array: GeometryArray) -> Result<GeometryArray> {
    match array {
        GeometryArray::WKB(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::Point(arr) => Ok(GeometryArray::Point(arr)),
        GeometryArray::MultiPoint(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::LineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiLineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
    }
}
