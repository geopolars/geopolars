use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use polars::prelude::{Result, Series};

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

pub fn geom_at_index(series: &Series, index: usize) -> Result<Geometry<f64>> {
    let chunks = series.list().expect("series was not a list type");
    let row = chunks.into_iter().nth(index);
    let value = row.expect("Row is null").expect("Failed to get row");
    let buffer = value.u8().expect("Row is not of type u8");
    let vec: Vec<u8> = buffer.into_iter().map(|x| x.unwrap()).collect();
    let geom = Wkb(vec).to_geo().expect("unable to convert geo");
    Ok(geom)
}

pub enum Predicate {
    Intersects,
    Contains,
}
