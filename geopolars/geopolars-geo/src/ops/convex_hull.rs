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
    use crate::util::iter_geom;
    use geo::{line_string, polygon, Geometry, MultiPoint, Point};
    use geopolars_arrow::linestring::MutableLineStringArray;
    use geopolars_arrow::polygon::PolygonSeries;
    use geozero::{CoordDimensions, ToWkb};
    use polars::export::arrow::array::{Array, ListArray};
    use polars::export::arrow::array::{BinaryArray, MutableBinaryArray};
    use polars::prelude::Series;

    #[test]
    fn convex_hull_for_multipoint() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        // Values borrowed from this test in geo crate: https://docs.rs/geo/0.14.2/src/geo/algorithm/convexhull.rs.html#323
        let v = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 0.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];
        let mp = MultiPoint(v);

        let correct_poly: Geometry<f64> = polygon![
            (x:0.0, y: -10.0),
            (x:10.0, y: 0.0),
            (x:0.0, y:10.0),
            (x:-10.0, y:0.0),
            (x:0.0, y:-10.0),
        ]
        .into();

        let test_geom: Geometry<f64> = mp.into();
        let test_wkb = test_geom.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let convex_res = series.convex_hull();

        assert!(
            convex_res.is_ok(),
            "Should get a valid result back from convex hull"
        );
        let convex_res = convex_res.unwrap();

        assert_eq!(
            convex_res.len(),
            1,
            "Should get a single result back from the series"
        );
        let mut geom_iter = iter_geom(&convex_res);
        let result = geom_iter.next().unwrap();

        assert_eq!(result, correct_poly, "Should get the correct convex hull");
    }

    #[test]
    fn convex_hull_linestring_test() {
        let line_strings = vec![line_string![
            (x: 0.0, y: 10.0),
            (x: 1.0, y: 1.0),
            (x: 10.0, y: 0.0),
            (x: 1.0, y: -1.0),
            (x: 0.0, y: -10.0),
            (x: -1.0, y: -1.0),
            (x: -10.0, y: 0.0),
            (x: -1.0, y: 1.0),
            (x: 0.0, y: 10.0),
        ]];
        let expected = polygon![
            (x: 0.0, y: -10.0),
            (x: 10.0, y: 0.0),
            (x: 0.0, y: 10.0),
            (x: -10.0, y: 0.0),
            (x: 0.0, y: -10.0),
        ];

        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series.convex_hull().unwrap();
        let actual_geo = PolygonSeries(&actual).get_as_geo(0).unwrap();
        assert_eq!(actual_geo, expected);
    }
}
