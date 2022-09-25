use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use polars::{
    datatypes::AnyValue,
    prelude::{PolarsError, Result, Series},
};

/// Helper function to iterate over geometries from polars Series
pub(crate) fn iter_geom(series: &Series) -> impl Iterator<Item = Geometry<f64>> + '_ {
    let chunks = series.list().expect("series was not a list type");
    let iter = chunks.into_iter();
    iter.map(|row| {
        let value = row.expect("Row is null");
        let buffer = value.u8().expect("Row is not type u8");
        let vec: Vec<u8> = buffer.into_iter().map(|x| x.unwrap()).collect();
        Wkb(vec).to_geo().expect("unable to convert to geo")
    })
}

// Parse a u8 series representing a single WKB geometry to a Geometry object
pub(crate) fn parse_u8_series_to_geom(row: &Series) -> Result<Geometry> {
    let buffer = row.u8()?;
    let vec = buffer.cont_slice()?.to_vec();
    let geom = Wkb(vec).to_geo().expect("unable to convert geo");
    Ok(geom)
}

/// Access to a geometry at a specified index
pub(crate) fn geom_at_index(series: &Series, index: usize) -> Result<Geometry<f64>> {
    let item_at_index = match series.get(index) {
        AnyValue::List(buf) => buf,
        _ => return Err(PolarsError::SchemaMisMatch("".into())),
    };

    let buffer = item_at_index.u8()?;
    let vec: Vec<u8> = buffer.into_iter().map(|x| x.unwrap()).collect();
    let geom = Wkb(vec).to_geo().expect("unable to convert geo");
    Ok(geom)
}

pub enum Predicate {
    Intersects,
    Contains,
}
