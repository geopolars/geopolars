use std::sync::Arc;

use crate::util::iter_geom;
use arrow2::array::{
    ArrayRef, BinaryArray, BooleanArray, MutableBinaryArray, MutableBooleanArray,
    MutablePrimitiveArray, PrimitiveArray,
};
use geo::{Geometry, Point};
use geozero::{CoordDimensions, ToWkb};
use polars::prelude::{PolarsError, Result, Series};

pub trait GeoSeries {
    fn area(&self) -> Result<Series>;

    fn centroid(&self) -> Result<Series>;

    /// Returns a boolean Series with value True for empty geometries
    fn is_empty(&self) -> Result<Series>;

    /// Return the x location of point geometries in a GeoSeries
    fn x(&self) -> Result<Series>;

    /// Return the y location of point geometries in a GeoSeries
    fn y(&self) -> Result<Series>;
}

impl GeoSeries for Series {
    fn area(&self) -> Result<Series> {
        use geo::prelude::Area;

        let output_series: Series = iter_geom(self).map(|geom| geom.unsigned_area()).collect();

        Ok(output_series)
    }

    fn centroid(&self) -> Result<Series> {
        use geo::algorithm::centroid::Centroid;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let value: Geometry<f64> = geom.centroid().expect("could not create centroid").into();
            let wkb = value
                .to_wkb(CoordDimensions::xy())
                .expect("Unable to create wkb");

            output_array.push(Some(wkb));
        }

        let result: BinaryArray<i32> = output_array.into();

        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    fn is_empty(&self) -> Result<Series> {
        use geo::dimensions::HasDimensions;

        let mut result = MutableBooleanArray::with_capacity(self.len());

        for geom in iter_geom(self) {
            result.push(Some(geom.is_empty()));
        }

        let result: BooleanArray = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn x(&self) -> Result<Series> {
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let point: Point<f64> = match geom.try_into() {
                Ok(point) => point,
                Err(_) => {
                    return Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Not a point geometry",
                    )))
                }
            };
            result.push(Some(point.x()));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn y(&self) -> Result<Series> {
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let point: Point<f64> = match geom.try_into() {
                Ok(point) => point,
                Err(_) => {
                    return Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Not a point geometry",
                    )))
                }
            };
            result.push(Some(point.y()));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }
}
