use crate::error::Result;
use crate::util::{from_geom_vec, iter_geom};
use geo::Geometry;
use polars::prelude::Series;

pub(crate) fn explode(series: &Series) -> Result<Series> {
    explode_wkb(series)
}

fn explode_wkb(series: &Series) -> Result<Series> {
    let mut exploded_vector = Vec::new();

    for geometry in iter_geom(series) {
        match geometry {
            Geometry::Point(geometry) => {
                let point = Geometry::Point(geometry);
                exploded_vector.push(point)
            }
            Geometry::MultiPoint(geometry) => {
                for geom in geometry.into_iter() {
                    let point = Geometry::Point(geom);
                    exploded_vector.push(point)
                }
            }
            Geometry::Line(geometry) => {
                let line = Geometry::Line(geometry);
                exploded_vector.push(line)
            }
            Geometry::LineString(geometry) => {
                let line_string = Geometry::LineString(geometry);
                exploded_vector.push(line_string)
            }
            Geometry::MultiLineString(geometry) => {
                for geom in geometry.into_iter() {
                    let line_string = Geometry::LineString(geom);
                    exploded_vector.push(line_string)
                }
            }
            Geometry::Polygon(geometry) => {
                let polygon = Geometry::Polygon(geometry);
                exploded_vector.push(polygon)
            }
            Geometry::MultiPolygon(geometry) => {
                for geom in geometry.into_iter() {
                    let polygon = Geometry::Polygon(geom);
                    exploded_vector.push(polygon)
                }
            }
            Geometry::Rect(geometry) => {
                let rectangle = Geometry::Rect(geometry);
                exploded_vector.push(rectangle)
            }
            Geometry::Triangle(geometry) => {
                let triangle = Geometry::Triangle(geometry);
                exploded_vector.push(triangle)
            }
            _ => unimplemented!(),
        };
    }

    let exploded_series = from_geom_vec(&exploded_vector)?;

    Ok(exploded_series)
}
