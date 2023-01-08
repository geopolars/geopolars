use crate::error::Result;
use crate::geoarrow::linestring::array::LineStringSeries;
use crate::geoarrow::point::array::PointSeries;
use crate::geoarrow::polygon::array::PolygonSeries;
use crate::util::iter_geom;
use geo::{Geometry, LineString, Point, Polygon};
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::Series;

pub fn map_point_series_to_float_series<F>(series: &Series, op: F) -> Result<Series>
where
    F: Fn(Point) -> f64,
{
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    let series = PointSeries(series);

    for point_array in series.chunks() {
        let parts = point_array.parts();
        for i in 0..parts.len() {
            let point = parts.get_as_geo(i);
            result.push(point.map(&op))
        }
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

pub fn map_linestring_series_to_float_series<F>(series: &Series, op: F) -> Result<Series>
where
    F: Fn(LineString) -> f64,
{
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    let series = LineStringSeries(series);

    for line_string_array in series.chunks() {
        let parts = line_string_array.parts();
        for i in 0..parts.len() {
            let line_string = parts.get_as_geo(i);
            result.push(line_string.map(&op))
        }
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

pub fn map_polygon_series_to_float_series<F>(series: &Series, op: F) -> Result<Series>
where
    F: Fn(Polygon) -> f64,
{
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    let series = PolygonSeries(series);

    for polygon_array in series.chunks() {
        let parts = polygon_array.parts();
        for i in 0..parts.len() {
            let polygon = parts.get_as_geo(i);
            result.push(polygon.map(&op))
        }
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

pub fn map_wkb_series_to_float_series<F>(series: &Series, op: F) -> Result<Series>
where
    F: Fn(Geometry) -> f64,
{
    let result: Vec<f64> = iter_geom(series).map(op).collect();
    let series = Series::try_from((
        "geometry",
        Box::new(PrimitiveArray::<f64>::from_vec(result)) as Box<dyn Array>,
    ))?;
    Ok(series)
}
