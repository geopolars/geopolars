use crate::error::Result;
use geo::Geometry;
use geopolars_arrow::GeometryArrayEnum;
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};
use polars::export::arrow::datatypes::DataType;

pub(crate) fn x(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    match array {
        GeometryArrayEnum::Point(arr) => Ok(PrimitiveArray::<f64>::new(
            DataType::Float64,
            arr.values_x().clone(),
            arr.validity().cloned(),
        )),
        GeometryArrayEnum::WKB(arr) => {
            let mut output_arr = MutablePrimitiveArray::<f64>::with_capacity(arr.len());
            arr.iter_geo().for_each(|maybe_geom| {
                let maybe_point = maybe_geom.map(|geom| match geom {
                    Geometry::Point(pt) => pt,
                    _ => panic!("x only implemented for points"),
                });
                output_arr.push(maybe_point.map(|pt| pt.x()))
            });
            Ok(output_arr.into())
        }
        _ => panic!("Unexpected geometry type for operation x"),
    }
}

pub(crate) fn y(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    match array {
        GeometryArrayEnum::Point(arr) => Ok(PrimitiveArray::<f64>::new(
            DataType::Float64,
            arr.values_y().clone(),
            arr.validity().cloned(),
        )),
        GeometryArrayEnum::WKB(arr) => {
            let mut output_arr = MutablePrimitiveArray::<f64>::with_capacity(arr.len());
            arr.iter_geo().for_each(|maybe_geom| {
                let maybe_point = maybe_geom.map(|geom| match geom {
                    Geometry::Point(pt) => pt,
                    _ => panic!("x only implemented for points"),
                });
                output_arr.push(maybe_point.map(|pt| pt.y()))
            });
            Ok(output_arr.into())
        }
        _ => panic!("Unexpected geometry type for operation x"),
    }
}
