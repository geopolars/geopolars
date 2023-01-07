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

#[cfg(test)]
mod tests {
    use crate::geoseries::GeoSeries;
    use crate::util::from_geom_vec;
    use geo::{Geometry, LineString, Point};

    #[test]
    fn euclidean_distance() {
        let geo_series = from_geom_vec(&[
            Geometry::Point(Point::new(0.0, 0.0)),
            Geometry::Point(Point::new(0.0, 0.0)),
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::LineString(LineString::<f64>::from(vec![(0.0, 0.0), (0.0, 4.0)])),
        ])
        .unwrap();

        let other_geo_series = from_geom_vec(&[
            Geometry::Point(Point::new(0.0, 1.0)),
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::Point(Point::new(4.0, 5.0)),
            Geometry::Point(Point::new(2.0, 2.0)),
        ])
        .unwrap();
        let results = vec![1.0_f64, 2.0_f64.sqrt(), 5.0_f64, 2.0_f64];

        let distance_series = geo_series.distance(&other_geo_series);
        assert!(distance_series.is_ok(), "To get a series back");

        let distance_series = distance_series.unwrap();
        let distance_vec: Vec<f64> = distance_series.f64().unwrap().into_no_null_iter().collect();

        for (d1, d2) in distance_vec.iter().zip(results.iter()) {
            assert_eq!(d1, d2, "Distances differ, should be the same");
        }
    }
}
