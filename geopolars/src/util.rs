use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};
use polars::{
    datatypes::AnyValue,
    prelude::{PolarsError, PolarsResult, Series},
};

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

/// Access to a geometry at a specified index
pub(crate) fn geom_at_index(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let buffer = match series.get(index) {
        AnyValue::Binary(buf) => buf,
        _ => return Err(PolarsError::SchemaMisMatch("".into())),
    };

    let geom = Wkb(buffer.to_vec())
        .to_geo()
        .expect("unable to convert geo");
    Ok(geom)
}

pub enum Predicate {
    Intersects,
    Contains,
}
