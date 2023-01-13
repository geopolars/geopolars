use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::simplify::Simplify;
use geo::{Geometry, LineString, Polygon};
use geopolars_arrow::linestring::LineStringSeries;
use geopolars_arrow::linestring::MutableLineStringArray;
use geopolars_arrow::polygon::MutablePolygonArray;
use geopolars_arrow::polygon::PolygonSeries;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::{ListChunked, Series};
use polars::series::IntoSeries;

pub(crate) fn simplify(series: &Series, tolerance: f64) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => simplify_wkb(series, tolerance),
        GeoArrowType::Point => Ok(series.clone()),
        GeoArrowType::LineString => simplify_geoarrow_linestring(series, tolerance),
        GeoArrowType::Polygon => simplify_geoarrow_polygon(series, tolerance),
    }
}

fn simplify_wkb(series: &Series, tolerance: f64) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value = match geom {
            Geometry::Point(g) => Geometry::Point(g),
            Geometry::MultiPoint(g) => Geometry::MultiPoint(g),
            Geometry::LineString(g) => Geometry::LineString(g.simplify(&tolerance)),
            Geometry::MultiLineString(g) => Geometry::MultiLineString(g.simplify(&tolerance)),
            Geometry::Polygon(g) => Geometry::Polygon(g.simplify(&tolerance)),
            Geometry::MultiPolygon(g) => Geometry::MultiPolygon(g.simplify(&tolerance)),
            _ => unimplemented!(),
        };

        let wkb = value
            .to_wkb(CoordDimensions::xy())
            .expect("Unable to create wkb");

        output_array.push(Some(wkb));
    }

    let result: BinaryArray<i32> = output_array.into();

    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

fn simplify_geoarrow_linestring(series: &Series, tolerance: f64) -> Result<Series> {
    let mut output_chunks: Vec<Box<dyn Array>> = vec![];
    for chunk in LineStringSeries(series).chunks() {
        let out: Vec<Option<LineString>> = chunk
            .parts()
            .iter_geo()
            .map(|maybe_geo| maybe_geo.map(|g| g.simplify(&tolerance)))
            .collect();
        let mut_arr: MutableLineStringArray = out.into();
        output_chunks.push(Box::new(mut_arr.into_arrow()) as Box<dyn Array>);
    }

    Ok(ListChunked::from_chunks("result", output_chunks).into_series())
}

fn simplify_geoarrow_polygon(series: &Series, tolerance: f64) -> Result<Series> {
    let mut output_chunks: Vec<Box<dyn Array>> = vec![];
    for chunk in PolygonSeries(series).chunks() {
        let out: Vec<Option<Polygon>> = chunk
            .parts()
            .iter_geo()
            .map(|maybe_geo| maybe_geo.map(|g| g.simplify(&tolerance)))
            .collect();
        let mut_arr: MutablePolygonArray = out.into();
        output_chunks.push(Box::new(mut_arr.into_arrow()) as Box<dyn Array>);
    }

    Ok(ListChunked::from_chunks("result", output_chunks).into_series())
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
