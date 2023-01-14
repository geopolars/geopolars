use crate::error::Result;
use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use geozero::{CoordDimensions, ToWkb};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
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
