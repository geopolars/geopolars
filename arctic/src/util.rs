use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use polars::prelude::Series;

/// Helper function to iterate over geometries from polars Series
pub fn iter_geom(series: &Series) -> impl Iterator<Item = Geometry<f64>> + '_ {
    let chunks = series.list().expect("series was not a list type");
    let iter = chunks.into_iter();
    iter.map(|row| {
        let value = row.expect("Row is null");
        let buffer = value.u8().expect("Row is not type u8");
        let vec: Vec<u8> = buffer.into_iter().map(|x| x.unwrap()).collect();
        Wkb(vec).to_geo().expect("unable to convert to geo")
    })
}
