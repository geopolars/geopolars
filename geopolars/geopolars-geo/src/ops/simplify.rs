use crate::error::Result;
use geo::algorithm::simplify::Simplify;
use geo::{Geometry, LineString, MultiLineString, MultiPolygon, Polygon};
use geoarrow::GeometryArray;

pub(crate) fn simplify(array: GeometryArray, tolerance: &f64) -> Result<GeometryArray> {
    match array {
        GeometryArray::WKB(arr) => {
            let output_geoms: Vec<Option<Geometry>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| simplify_geometry(geom, tolerance)))
                .collect();

            Ok(GeometryArray::WKB(output_geoms.into()))
        }
        GeometryArray::Point(arr) => Ok(GeometryArray::Point(arr)),
        GeometryArray::MultiPoint(arr) => Ok(GeometryArray::MultiPoint(arr)),
        GeometryArray::LineString(arr) => {
            let output_geoms: Vec<Option<LineString>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArray::LineString(output_geoms.into()))
        }
        GeometryArray::MultiLineString(arr) => {
            let output_geoms: Vec<Option<MultiLineString>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArray::MultiLineString(output_geoms.into()))
        }
        GeometryArray::Polygon(arr) => {
            let output_geoms: Vec<Option<Polygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArray::Polygon(output_geoms.into()))
        }
        GeometryArray::MultiPolygon(arr) => {
            let output_geoms: Vec<Option<MultiPolygon>> = arr
                .iter_geo()
                .map(|maybe_g| maybe_g.map(|geom| geom.simplify(tolerance)))
                .collect();

            Ok(GeometryArray::MultiPolygon(output_geoms.into()))
        }
    }
}

fn simplify_geometry(geom: Geometry, tolerance: &f64) -> Geometry {
    match geom {
        Geometry::Point(g) => Geometry::Point(g),
        Geometry::MultiPoint(g) => Geometry::MultiPoint(g),
        Geometry::LineString(g) => Geometry::LineString(g.simplify(tolerance)),
        Geometry::MultiLineString(g) => Geometry::MultiLineString(g.simplify(tolerance)),
        Geometry::Polygon(g) => Geometry::Polygon(g.simplify(tolerance)),
        Geometry::MultiPolygon(g) => Geometry::MultiPolygon(g.simplify(tolerance)),
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::simplify;
    use geo::{line_string, polygon, Geometry};
    use geoarrow::{GeometryArray, GeometryArrayTrait, LineStringArray, PolygonArray};

    #[test]
    fn rdp_test() {
        let input_geom = line_string![
            (x: 0.0, y: 0.0 ),
            (x: 5.0, y: 4.0 ),
            (x: 11.0, y: 5.5 ),
            (x: 17.3, y: 3.2 ),
            (x: 27.8, y: 0.1 ),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = simplify(GeometryArray::LineString(input_array), &1.0).unwrap();

        let expected = line_string![
            ( x: 0.0, y: 0.0 ),
            ( x: 5.0, y: 4.0 ),
            ( x: 11.0, y: 5.5 ),
            ( x: 27.8, y: 0.1 ),
        ];

        assert_eq!(
            Geometry::LineString(expected),
            result_array.get_as_geo(0).unwrap()
        );
    }

    #[test]
    fn polygon() {
        let input_geom = polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 5., y: 11.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ];
        let input_array: PolygonArray = vec![input_geom].into();
        let result_array = simplify(GeometryArray::Polygon(input_array), &2.0).unwrap();

        let expected = polygon![
            (x: 0., y: 0.),
            (x: 0., y: 10.),
            (x: 10., y: 10.),
            (x: 10., y: 0.),
            (x: 0., y: 0.),
        ];

        assert_eq!(
            Geometry::Polygon(expected),
            result_array.get_as_geo(0).unwrap()
        );
    }
}
