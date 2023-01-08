use crate::error::Result;
use crate::geoarrow::linestring::array::LineStringSeries;
use crate::geoarrow::linestring::mutable::MutableLineStringArray;
use crate::geoarrow::polygon::array::PolygonSeries;
use crate::geoarrow::polygon::mutable::MutablePolygonArray;
use crate::util::{get_geoarrow_type, iter_geom, GeoArrowType};
use geo::algorithm::simplify::Simplify;
use geo::{Geometry, LineString, Polygon};
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

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
    let mut output_geoms: Vec<Option<LineString>> = Vec::with_capacity(series.len());

    for chunk in LineStringSeries(series).chunks() {
        let parts = chunk.parts();
        for i in 0..parts.len() {
            output_geoms.push(parts.get_as_geo(i).map(|ls| ls.simplify(&tolerance)));
        }
    }

    let mut_linestring_arr: MutableLineStringArray = output_geoms.into();
    let series = Series::try_from((
        "geometry",
        Box::new(mut_linestring_arr.into_arrow()) as Box<dyn Array>,
    ))?;
    Ok(series)
}

fn simplify_geoarrow_polygon(series: &Series, tolerance: f64) -> Result<Series> {
    let mut output_geoms: Vec<Option<Polygon>> = Vec::with_capacity(series.len());

    for chunk in PolygonSeries(series).chunks() {
        let parts = chunk.parts();
        for i in 0..parts.len() {
            output_geoms.push(parts.get_as_geo(i).map(|ls| ls.simplify(&tolerance)));
        }
    }

    let mut_linestring_arr: MutablePolygonArray = output_geoms.into();
    let series = Series::try_from((
        "geometry",
        Box::new(mut_linestring_arr.into_arrow()) as Box<dyn Array>,
    ))?;
    Ok(series)
}

#[cfg(test)]
mod tests {
    use crate::geoarrow::linestring::array::LineStringSeries;
    use crate::geoarrow::linestring::mutable::MutableLineStringArray;
    use crate::geoseries::GeoSeries;
    use geo::line_string;
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
}
