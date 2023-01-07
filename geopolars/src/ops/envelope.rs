use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

pub(crate) fn envelope(series: &Series) -> Result<Series> {
    envelope_wkb(series)
}

fn envelope_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value: Geometry<f64> = geom.bounding_rect().unwrap().into();
        let wkb = value
            .to_wkb(CoordDimensions::xy())
            .expect("Unable to create wkb");

        output_array.push(Some(wkb));
    }

    let result: BinaryArray<i32> = output_array.into();

    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
