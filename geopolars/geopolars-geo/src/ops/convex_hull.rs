use crate::error::Result;
use geo::algorithm::convex_hull::ConvexHull;
use geo::Polygon;
use geopolars_arrow::GeometryArray;

pub(crate) fn convex_hull(array: GeometryArray) -> Result<GeometryArray> {
    match array {
        GeometryArray::WKB(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::Point(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }

        GeometryArray::MultiPoint(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::LineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiLineString(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.convex_hull()))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::convex_hull;
    use geo::{line_string, polygon, Geometry, MultiPoint, Point};
    use geopolars_arrow::{GeometryArray, GeometryArrayTrait, LineStringArray, MultiPointArray};

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
        let result_array = convex_hull(GeometryArray::MultiPoint(input_array)).unwrap();

        let expected = polygon![
            (x:0.0, y: -10.0),
            (x:10.0, y: 0.0),
            (x:0.0, y:10.0),
            (x:-10.0, y:0.0),
            (x:0.0, y:-10.0),
        ];

        assert_eq!(
            Geometry::Polygon(expected),
            result_array.get_as_geo(0).unwrap()
        );
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
        let result_array = convex_hull(GeometryArray::LineString(input_array)).unwrap();

        let expected = polygon![
            (x: 0.0, y: -10.0),
            (x: 10.0, y: 0.0),
            (x: 0.0, y: 10.0),
            (x: -10.0, y: 0.0),
            (x: 0.0, y: -10.0),
        ];

        assert_eq!(
            Geometry::Polygon(expected),
            result_array.get_as_geo(0).unwrap()
        );
    }
}
