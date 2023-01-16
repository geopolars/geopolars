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
    use super::convex_hull;
    use geo::{line_string, polygon, MultiPoint, Point};
    use geopolars_arrow::{GeometryArrayEnum, LineStringArray, MultiPointArray};

    #[test]
    fn convex_hull_for_multipoint() {
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
        let result_array = convex_hull(GeometryArrayEnum::MultiPoint(input_array)).unwrap();

        let result_array = match result_array {
            GeometryArrayEnum::Polygon(arr) => arr,
            _ => panic!(),
        };

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
        let result_array = convex_hull(GeometryArrayEnum::LineString(input_array)).unwrap();

        let result_array = match result_array {
            GeometryArrayEnum::Polygon(arr) => arr,
            _ => panic!(),
        };

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
