use std::sync::Arc;

use arrow2::array::{ArrayRef, BinaryArray, MutableBinaryArray};
use geozero::{
    wkb::{Decode, Wkb},
    CoordDimensions, ToGeo, ToWkb,
};
use polars::prelude::{Result, Series};

pub trait GeoSeries {
    fn centroid(&self) -> Result<Series>;
}

impl GeoSeries for Series {
    fn centroid(&self) -> Result<Series> {
        use geo::algorithm::centroid::Centroid;

        // TODO: add util for iterating over geometries
        let chunks = self.list().expect("series was not a list type");
        let iter = chunks.into_iter();

        let mut out_wkb = MutableBinaryArray::<i32>::with_capacity(self.len());

        for maybe_geom in iter {
            let geom = maybe_geom.expect("no geom?");
            let buf = geom.u8().expect("could not extract buf");
            let vec: Vec<u8> = buf.into_iter().map(|x| x.unwrap()).collect();
            let decoded_geom = Wkb(vec).to_geo().expect("unable to convert to geo");
            let center = decoded_geom.centroid().expect("could not create centroid");

            let geo_types_geom: geo::Geometry<f64> = center.into();
            let wkb = geo_types_geom
                .to_wkb(CoordDimensions::xy())
                .expect("Unable to create wkb");

            out_wkb.push(Some(wkb));
        }

        let result: BinaryArray<i32> = out_wkb.into();

        let out = Series::try_from(("geometry", Arc::new(result) as ArrayRef))?;
        println!("{}", out);

        Ok(out)
    }
}
