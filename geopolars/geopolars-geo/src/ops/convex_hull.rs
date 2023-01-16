use crate::error::Result;
use geo::algorithm::convex_hull::ConvexHull;
use geo::Polygon;
use geopolars_arrow::GeometryArrayEnum;

pub(crate) fn convex_hull(array: GeometryArrayEnum) -> Result<GeometryArrayEnum> {
    match array {
        GeometryArrayEnum::WKB(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::Point(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }

        GeometryArrayEnum::MultiPoint(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::LineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::geoseries::GeoSeries;
    use geo::{line_string, polygon, MultiPoint, Point};
    use geopolars_arrow::polygon::PolygonArray;
    use geopolars_arrow::{LineStringArray, MultiPointArray};
    use polars::prelude::Series;

    #[test]
    fn convex_hull_for_multipoint() {
        // NOTE: this actually gets interpreted as a LineString not MultiPoint due to inferring
        // type from arrow schema when parsing from a series

        // Values borrowed from this test in geo crate: https://docs.rs/geo/0.14.2/src/geo/algorithm/convexhull.rs.html#323
        let input_geom: MultiPoint = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 0.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ]
        .into();
        let input_array: MultiPointArray = vec![input_geom].into();
        let input_series =
            Series::try_from(("geometry", input_array.into_arrow().boxed())).unwrap();

        let result_series = input_series.convex_hull().unwrap();
        let result_array: PolygonArray = result_series.chunks()[0].try_into().unwrap();

        let expected = polygon![
            (x:0.0, y: -10.0),
            (x:10.0, y: 0.0),
            (x:0.0, y:10.0),
            (x:-10.0, y:0.0),
            (x:0.0, y:-10.0),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }

    #[test]
    fn convex_hull_linestring_test() {
        let input_geom = line_string![
            (x: 0.0, y: 10.0),
            (x: 1.0, y: 1.0),
            (x: 10.0, y: 0.0),
            (x: 1.0, y: -1.0),
            (x: 0.0, y: -10.0),
            (x: -1.0, y: -1.0),
            (x: -10.0, y: 0.0),
            (x: -1.0, y: 1.0),
            (x: 0.0, y: 10.0),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let input_series =
            Series::try_from(("geometry", input_array.into_arrow().boxed())).unwrap();

        let result_series = input_series.convex_hull().unwrap();
        let result_array: PolygonArray = result_series.chunks()[0].try_into().unwrap();

        let expected = polygon![
            (x: 0.0, y: -10.0),
            (x: 10.0, y: 0.0),
            (x: 0.0, y: 10.0),
            (x: -10.0, y: 0.0),
            (x: 0.0, y: -10.0),
        ];

        assert_eq!(expected, result_array.get_as_geo(0).unwrap());
    }
}
