use crate::error::{inner_type_name, GeopolarsError, Result};
use crate::util::iter_geom;
use geo::{Geometry, Point};
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::Series;

pub(crate) fn x(series: &Series) -> Result<Series> {
    x_wkb(series)
}

pub(crate) fn y(series: &Series) -> Result<Series> {
    y_wkb(series)
}

fn x_wkb(series: &Series) -> Result<Series> {
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let point: Point<f64> = match geom {
            Geometry::Point(point) => point,
            geom => {
                return Err(GeopolarsError::MismatchedGeometry {
                    expected: "Point",
                    found: inner_type_name(&geom),
                })
            }
        };
        result.push(Some(point.x()));
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

fn y_wkb(series: &Series) -> Result<Series> {
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let point: Point<f64> = match geom {
            Geometry::Point(point) => point,
            geom => {
                return Err(GeopolarsError::MismatchedGeometry {
                    expected: "Point",
                    found: inner_type_name(&geom),
                })
            }
        };
        result.push(Some(point.y()));
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}
