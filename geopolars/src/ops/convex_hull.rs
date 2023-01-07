use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::convex_hull::ConvexHull;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::{PolarsError, Series};

pub(crate) fn convex_hull(series: &Series) -> Result<Series> {
    convex_hull_wkb(series)
}

fn convex_hull_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let hull = match geom {
            Geometry::Polygon(polygon) => Ok(polygon.convex_hull()),
            Geometry::MultiPolygon(multi_poly) => Ok(multi_poly.convex_hull()),
            Geometry::MultiPoint(points) => Ok(points.convex_hull()),
            Geometry::LineString(line_string) => Ok(line_string.convex_hull()),
            Geometry::MultiLineString(multi_line_string) => Ok(multi_line_string.convex_hull()),
            _ => Err(PolarsError::ComputeError(ErrString::from(
                "ConvexHull not supported for this geometry type",
            ))),
        }?;
        let hull: Geometry<f64> = hull.into();
        let hull_wkb = hull.to_wkb(CoordDimensions::xy()).unwrap();

        output_array.push(Some(hull_wkb));
    }

    let result: BinaryArray<i32> = output_array.into();
    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
