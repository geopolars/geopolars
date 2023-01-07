use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::simplify::Simplify;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

pub(crate) fn simplify(series: &Series, tolerance: f64) -> Result<Series> {
    simplify_wkb(series, tolerance)
}

fn simplify_wkb(series: &Series, tolerance: f64) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value = match geom {
            Geometry::Point(g) => Geometry::Point(g),
            Geometry::MultiPoint(g) => Geometry::MultiPoint(g),
            Geometry::LineString(g) => Geometry::LineString(g.simplify(&tolerance)),
            Geometry::MultiLineString(g) => Geometry::MultiLineString(g.simplify(&tolerance)),
            Geometry::Polygon(g) => Geometry::Polygon(g.simplify(&tolerance)),
            Geometry::MultiPolygon(g) => Geometry::MultiPolygon(g.simplify(&tolerance)),
            _ => unimplemented!(),
        };

        let wkb = value
            .to_wkb(CoordDimensions::xy())
            .expect("Unable to create wkb");

        output_array.push(Some(wkb));
    }

    let result: BinaryArray<i32> = output_array.into();

    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
