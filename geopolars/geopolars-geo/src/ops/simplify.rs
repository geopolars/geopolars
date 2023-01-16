use crate::error::Result;
use geo::algorithm::simplify::Simplify;
use geo::{Geometry, LineString, MultiLineString, MultiPolygon, Polygon};
use geopolars_arrow::GeometryArrayEnum;

pub(crate) fn simplify(array: GeometryArrayEnum, tolerance: &f64) -> Result<GeometryArrayEnum> {
    match array {
        GeometryArrayEnum::WKB(arr) => {
            let output_geoms: Vec<Option<Geometry>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| simplify_geometry(geom, tolerance)))
                .collect();

            Ok(GeometryArrayEnum::WKB(output_geoms.into()))
        }
        GeometryArrayEnum::Point(arr) => Ok(GeometryArrayEnum::Point(arr)),
        GeometryArrayEnum::MultiPoint(arr) => Ok(GeometryArrayEnum::MultiPoint(arr)),
        GeometryArrayEnum::LineString(arr) => {
            let output_geoms: Vec<Option<LineString>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArrayEnum::LineString(output_geoms.into()))
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            let output_geoms: Vec<Option<MultiLineString>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArrayEnum::MultiLineString(output_geoms.into()))
        }
        GeometryArrayEnum::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArrayEnum::Polygon(output_geoms.into()))
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<MultiPolygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArrayEnum::MultiPolygon(output_geoms.into()))
        }
    }
}

fn simplify_geometry(geom: Geometry, tolerance: &f64) -> Geometry {
    match geom {
        Geometry::Point(g) => Geometry::Point(g),
        Geometry::MultiPoint(g) => Geometry::MultiPoint(g),
        Geometry::LineString(g) => Geometry::LineString(g.simplify(&tolerance)),
        Geometry::MultiLineString(g) => Geometry::MultiLineString(g.simplify(&tolerance)),
        Geometry::Polygon(g) => Geometry::Polygon(g.simplify(&tolerance)),
        Geometry::MultiPolygon(g) => Geometry::MultiPolygon(g.simplify(&tolerance)),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use crate::geoseries::GeoSeries;
    use geo::{line_string, polygon};
    use geopolars_arrow::linestring::LineStringSeries;
    use geopolars_arrow::linestring::MutableLineStringArray;
    use geopolars_arrow::polygon::MutablePolygonArray;
    use geopolars_arrow::polygon::PolygonSeries;
    use polars::export::arrow::array::{Array, ListArray};
    use polars::prelude::Series;

    #[test]
    fn rdp_test() {
        let line_strings = vec![line_string![
            (x: 0.0, y: 0.0 ),
            (x: 5.0, y: 4.0 ),
            (x: 11.0, y: 5.5 ),
            (x: 17.3, y: 3.2 ),
            (x: 27.8, y: 0.1 ),
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series.simplify(1.0).unwrap();
        let actual_geo = LineStringSeries(&actual).get_as_geo(0).unwrap();

        let expected = line_string![
            ( x: 0.0, y: 0.0 ),
            ( x: 5.0, y: 4.0 ),
            ( x: 11.0, y: 5.5 ),
            ( x: 27.8, y: 0.1 ),
        ];
        assert_eq!(actual_geo, expected);
    }

    #[test]
    fn polygon() {
        let polys = vec![polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 5., y: 11.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ]];
        let mut_poly_arr: MutablePolygonArray = polys.into();

        let poly_arr = mut_poly_arr.into_arrow();
        let series = Series::try_from(("geometry", Box::new(poly_arr) as Box<dyn Array>)).unwrap();

        let actual = series.simplify(2.0).unwrap();
        let actual_geo = PolygonSeries(&actual).get_as_geo(0).unwrap();

        let expected = polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ];

        assert_eq!(actual_geo, expected);
    }
}
