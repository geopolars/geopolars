use crate::error::Result;
use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use geozero::{CoordDimensions, ToWkb};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::export::arrow::compute::concatenate::concatenate;
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

// This is a workaround hack because StructChunked::from_chunks doesn't exist
pub fn struct_series_from_chunks(chunks: Vec<Box<dyn Array>>) -> Result<Series> {
    let refs: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk.as_ref()).collect();
    let output = concatenate(refs.as_slice()).unwrap();
    Ok(Series::try_from(("geometry", output))?)
}

// This is a temporary workaround instead of remembering when to call StructChunked::from_chunks,
// ListChunked::from_chunks, and BinaryChunked::from_chunks depending on the geometry type of the
// column returned from a generic operation like simplify
pub fn series_from_any_chunks(chunks: Vec<Box<dyn Array>>) -> Result<Series> {
    let refs: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk.as_ref()).collect();
    let output = concatenate(refs.as_slice()).unwrap();
    Ok(Series::try_from(("geometry", output))?)
}
