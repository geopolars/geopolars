use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::EuclideanDistance;
use geo::Geometry;
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::Series;

pub(crate) fn euclidean_distance(series: &Series, other: &Series) -> Result<Series> {
    euclidean_distance_wkb(series, other)
}

fn euclidean_distance_wkb(series: &Series, other: &Series) -> Result<Series> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    for (g1, g2) in iter_geom(series).zip(iter_geom(other)) {
        let distance = match (g1, g2) {
            (Geometry::Point(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Point(p1), Geometry::MultiPoint(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Point(p1), Geometry::Line(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Point(p1), Geometry::LineString(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Point(p1), Geometry::MultiLineString(p2)) => {
                Some(p1.euclidean_distance(&p2))
            }
            (Geometry::Point(p1), Geometry::Polygon(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Point(p1), Geometry::MultiPolygon(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::MultiPoint(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),

            (Geometry::Line(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Line(p1), Geometry::Line(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Line(p1), Geometry::LineString(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Line(p1), Geometry::Polygon(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Line(p1), Geometry::MultiPolygon(p2)) => Some(p1.euclidean_distance(&p2)),

            (Geometry::LineString(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::LineString(p1), Geometry::Line(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::LineString(p1), Geometry::LineString(p2)) => {
                Some(p1.euclidean_distance(&p2))
            }
            (Geometry::LineString(p1), Geometry::Polygon(p2)) => Some(p1.euclidean_distance(&p2)),

            (Geometry::MultiLineString(p1), Geometry::Point(p2)) => {
                Some(p1.euclidean_distance(&p2))
            }

            (Geometry::Polygon(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Polygon(p1), Geometry::Line(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Polygon(p1), Geometry::LineString(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => Some(p1.euclidean_distance(&p2)),

            (Geometry::MultiPolygon(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            (Geometry::MultiPolygon(p1), Geometry::Line(p2)) => Some(p1.euclidean_distance(&p2)),

            (Geometry::Triangle(p1), Geometry::Point(p2)) => Some(p1.euclidean_distance(&p2)),
            _ => None,
        };
        output_array.push(distance);
    }

    let result: PrimitiveArray<f64> = output_array.into();
    let series = Series::try_from(("distance", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
