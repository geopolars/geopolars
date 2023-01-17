use crate::error::Result;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::Polygon;
use geopolars_arrow::GeometryArrayEnum;

pub(crate) fn envelope(array: GeometryArrayEnum) -> Result<GeometryArrayEnum> {
    match array {
        GeometryArrayEnum::WKB(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::Point(arr) => Ok(GeometryArrayEnum::Point(arr)),
        GeometryArrayEnum::MultiPoint(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::LineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| {
                    maybe_g.and_then(|geom| geom.bounding_rect().map(|rect| rect.to_polygon()))
                })
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
    }
}
