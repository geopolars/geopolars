use crate::error::Result;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

use crate::util::iter_geom;

pub(crate) fn exterior(series: &Series) -> Result<Series> {
    exterior_wkb(series)
}

fn exterior_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let maybe_exterior = match geom {
            Geometry::Polygon(polygon) => {
                let exterior: Geometry<f64> = polygon.exterior().clone().into();
                Some(exterior.to_wkb(CoordDimensions::xy()).unwrap())
            }
            _ => None,
        };
        output_array.push(maybe_exterior);
    }

    let result: BinaryArray<i32> = output_array.into();

    Ok(Series::try_from((
        "geometry",
        Box::new(result) as Box<dyn Array>,
    ))?)
}
