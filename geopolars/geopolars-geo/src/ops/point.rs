use crate::error::{inner_type_name, GeopolarsError, Result};
use crate::util::iter_geom;
use geo::{Geometry, Point};
use geopolars_arrow::point::PointSeries;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::{Float64Chunked, Series};
use polars::series::IntoSeries;

pub(crate) fn x(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => x_wkb(series),
        GeoArrowType::Point => x_geoarrow(series),
        _ => panic!("Unexpected geometry type for operation x"),
    }
}

pub(crate) fn y(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => y_wkb(series),
        GeoArrowType::Point => y_geoarrow(series),
        _ => panic!("Unexpected geometry type for operation y"),
    }
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

fn x_geoarrow(series: &Series) -> Result<Series> {
    let output_chunks: Vec<Box<dyn Array>> = PointSeries(series)
        .chunks()
        .into_iter()
        .map(|point_arr| Box::new(point_arr.parts().x.clone()) as Box<dyn Array>)
        .collect();
    Ok(Float64Chunked::from_chunks("result", output_chunks).into_series())
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

fn y_geoarrow(series: &Series) -> Result<Series> {
    let output_chunks: Vec<Box<dyn Array>> = PointSeries(series)
        .chunks()
        .into_iter()
        .map(|point_arr| Box::new(point_arr.parts().y.clone()) as Box<dyn Array>)
        .collect();
    Ok(Float64Chunked::from_chunks("result", output_chunks).into_series())
}
