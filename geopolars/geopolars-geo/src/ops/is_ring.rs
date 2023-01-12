use crate::error::Result;
use crate::util::iter_geom;
use geo::Geometry;
use polars::export::arrow::array::{Array, BooleanArray, MutableBooleanArray};
use polars::prelude::Series;

pub(crate) fn is_ring(series: &Series) -> Result<Series> {
    is_ring_wkb(series)
}

fn is_ring_wkb(series: &Series) -> Result<Series> {
    let mut result = MutableBooleanArray::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value = match geom {
            Geometry::LineString(g) => Some(g.is_closed()),
            Geometry::MultiLineString(g) => Some(g.is_closed()),
            _ => None,
        };
        result.push(value);
    }

    let result: BooleanArray = result.into();
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
