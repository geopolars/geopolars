use crate::error::Result;
use crate::util::iter_geom;
use geo::prelude::Area;
use polars::prelude::Series;

pub(crate) fn area(series: &Series) -> Result<Series> {
    area_wkb(series)
}

fn area_wkb(series: &Series) -> Result<Series> {
    let output_series: Series = iter_geom(series).map(|geom| geom.unsigned_area()).collect();

    Ok(output_series)
}
