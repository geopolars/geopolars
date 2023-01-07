use crate::error::Result;
use crate::util::iter_geom;
use geo::Geometry;
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::Series;

pub(crate) fn geom_type(series: &Series) -> Result<Series> {
    geom_type_wkb(series)
}

fn geom_type_wkb(series: &Series) -> Result<Series> {
    let mut result = MutablePrimitiveArray::<i8>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let type_id: i8 = match geom {
            Geometry::Point(_) => 0,
            Geometry::Line(_) => 1,
            Geometry::LineString(_) => 1,
            Geometry::Polygon(_) => 3,
            Geometry::MultiPoint(_) => 4,
            Geometry::MultiLineString(_) => 5,
            Geometry::MultiPolygon(_) => 6,
            Geometry::GeometryCollection(_) => 7,
            // Should these still call themselves polygon?
            Geometry::Rect(_) => 3,
            Geometry::Triangle(_) => 3,
        };
        result.push(Some(type_id));
    }

    let result: PrimitiveArray<i8> = result.into();
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
