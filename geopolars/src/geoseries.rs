use std::sync::Arc;

use crate::util::iter_geom;
use arrow2::{
    array::{
        ArrayRef, BinaryArray, FixedSizeListArray, MutableBinaryArray,
        MutableFixedSizeListArray, MutablePrimitiveArray, TryExtend,
    },
};
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::prelude::{Result, Series};

pub trait GeoSeries {
    fn area(&self) -> Result<Series>;

    fn bounds(&self) -> Result<Series>;

    fn centroid(&self) -> Result<Series>;
}

impl GeoSeries for Series {
    fn area(&self) -> Result<Series> {
        use geo::prelude::Area;

        let output_series: Series = iter_geom(self).map(|geom| geom.unsigned_area()).collect();

        Ok(output_series)
    }

    fn bounds(&self) -> Result<Series> {
        use geo::algorithm::bounding_rect::BoundingRect;

        let mut output_vec: Vec<Option<Vec<Option<f64>>>> = Vec::with_capacity(self.len() * 4);

        for geom in iter_geom(self) {
            let value = geom.bounding_rect().expect("could not create centroid");
            let mut item: Vec<Option<f64>> = Vec::with_capacity(4);
            item.push(Some(value.min().x));
            item.push(Some(value.min().y));
            item.push(Some(value.max().x));
            item.push(Some(value.max().y));
            output_vec.push(Some(item));
        }

        let mut list = MutableFixedSizeListArray::new(MutablePrimitiveArray::<f64>::new(), 4);
        list.try_extend(output_vec).unwrap();
        let list: FixedSizeListArray = list.into();

        Series::try_from(("result", Arc::new(list) as ArrayRef))
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
}
