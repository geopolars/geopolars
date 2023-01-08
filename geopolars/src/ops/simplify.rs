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
