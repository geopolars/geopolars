use crate::error::Result;
use crate::util::iter_geom;
use geo::dimensions::HasDimensions;
use polars::export::arrow::array::{Array, BooleanArray, MutableBooleanArray};
use polars::prelude::Series;

pub(crate) fn is_empty(series: &Series) -> Result<Series> {
    is_empty_wkb(series)
}

fn is_empty_wkb(series: &Series) -> Result<Series> {
    let mut result = MutableBooleanArray::with_capacity(series.len());

    for geom in iter_geom(series) {
        result.push(Some(geom.is_empty()));
    }

    let result: BooleanArray = result.into();
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
