use crate::error::Result;
use geo::{Geometry, LineString, Point, Polygon};
use geopolars_arrow::linestring::array::LineStringSeries;
use geopolars_arrow::point::array::PointSeries;
use geopolars_arrow::polygon::array::PolygonSeries;
use geozero::{wkb::Wkb, ToGeo};
use geozero::{CoordDimensions, ToWkb};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::{PolarsError, Series};
use std::convert::Into;

pub fn from_geom_vec(geoms: &[Geometry<f64>]) -> Result<Series> {
    let mut wkb_array = MutableBinaryArray::<i32>::with_capacity(geoms.len());

    for geom in geoms {
        let wkb = geom.to_wkb(CoordDimensions::xy()).map_err(|_| {
            PolarsError::ComputeError(ErrString::from("Failed to convert geom vec to GeoSeries"))
        })?;
        wkb_array.push(Some(wkb));
    }
    let array: BinaryArray<i32> = wkb_array.into();

    let series = Series::try_from(("geometry", Box::new(array) as Box<dyn Array>))?;
    Ok(series)
}

/// Helper function to iterate over geometries from polars Series
pub(crate) fn iter_geom(series: &Series) -> impl Iterator<Item = Geometry<f64>> + '_ {
    let chunks = series.binary().expect("series was not a list type");

    let iter = chunks.into_iter();
    iter.map(|row| {
        let value = row.expect("Row is null");
        Wkb(value.to_vec())
            .to_geo()
            .expect("unable to convert to geo")
    })
}

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
