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


    //     let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

    //     match get_geoarrow_type(self) {
    //         GeoArrowType::Point => {
    //             for chunk in self.chunks().iter() {
    //                 let struct_chunk = chunk.as_any().downcast_ref::<StructArray>().unwrap();
    //                 let x_array = struct_chunk.values()[0]
    //                     .as_any()
    //                     .downcast_ref::<PrimitiveArray<f64>>()
    //                     .unwrap();
    //                 for x in x_array {
    //                     result.push(x.cloned())
    //                 }
    //             }
    //         }
    //         GeoArrowType::WKB => {
    //             for geom in iter_geom(self) {
    //                 let point: Point<f64> = match geom {
    //                     Geometry::Point(point) => point,
    //                     geom => {
    //                         return Err(GeopolarsError::MismatchedGeometry {
    //                             expected: "Point",
    //                             found: inner_type_name(&geom),
    //                         })
    //                     }
    //                 };
    //                 result.push(Some(point.x()));
    //             }
    //         }
    //         _ => {
    //             return Err(GeopolarsError::MismatchedGeometry {
    //                 expected: "Point",
    //                 found: "todo",
    //             })
    //         }
    //     }

    //     let result: PrimitiveArray<f64> = result.into();
    //     let series = Series::try_from(("result", Box::new(result) as ArrayRef))?;
    //     Ok(series)
    // }

    // fn y(&self) -> Result<Series> {
    //     let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

    //     match get_geoarrow_type(self) {
    //         GeoArrowType::Point => {
    //             for chunk in self.chunks().iter() {
    //                 let struct_chunk = chunk.as_any().downcast_ref::<StructArray>().unwrap();
    //                 let x_array = struct_chunk.values()[1]
    //                     .as_any()
    //                     .downcast_ref::<PrimitiveArray<f64>>()
    //                     .unwrap();
    //                 for x in x_array {
    //                     result.push(x.cloned())
    //                 }
    //             }
    //         }
    //         GeoArrowType::WKB => {
    //             for geom in iter_geom(self) {
    //                 let point: Point<f64> = match geom {
    //                     Geometry::Point(point) => point,
    //                     geom => {
    //                         return Err(GeopolarsError::MismatchedGeometry {
    //                             expected: "Point",
    //                             found: inner_type_name(&geom),
    //                         })
    //                     }
    //                 };
    //                 result.push(Some(point.x()));
    //             }
    //         }
    //         _ => {
    //             return Err(GeopolarsError::MismatchedGeometry {
    //                 expected: "Point",
    //                 found: "todo",
    //             })
    //         }
    //     }

    //     let result: PrimitiveArray<f64> = result.into();
    //     let series = Series::try_from(("result", Box::new(result) as ArrayRef))?;
    //     Ok(series)
    // }
