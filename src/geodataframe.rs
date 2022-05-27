use geozero::{
    wkb::{Decode, Wkb},
    ToGeo, CoordDimensions, ToWkb,
};
use polars::{prelude::{DataFrame, IntoVec, Result, Series, NamedFrom}, chunked_array::ChunkedArray, datatypes::ListType};

pub trait GeoDataFrame {
    fn centroid(&self) -> Result<()>;

    fn hello_world(&self) -> Result<()>;

    // fn area(&self) -> Result<Series>;
}

impl GeoDataFrame for DataFrame {
    fn centroid(&self) -> Result<()> {
        use geo::algorithm::centroid::Centroid;

        let geom_column = self.column("geometry")?;

        let chunks = geom_column.list().expect("series was not a list type");
        let iter = chunks.into_iter();

        let mut out_wkb: Vec<Vec<u8>> = Vec::new();
        for maybe_geom in iter {
            let geom = maybe_geom.expect("no geom?");
            let buf = geom.u8().expect("could not extract buf");
            let vec: Vec<u8> = buf.into_iter().map(|x| x.unwrap()).collect();
            let decoded_geom = Wkb(vec).to_geo().expect("unable to convert to geo");
            let center = decoded_geom.centroid().expect("could not create centroid");

            let geo_types_geom: geo::Geometry<f64> = center.into();
            let wkb = geo_types_geom.to_wkb(CoordDimensions::xy()).expect("Unable to create wkb");

            out_wkb.push(wkb);
        }

        // TODO: need to figure out how to reconstruct a series
        let test = Series::try_from(out_wkb);
        // let test: ChunkedArray<ListType> = ChunkedArray::from_vec("centroid", out_wkb);


        Ok(())
    }

    fn hello_world(&self) -> Result<()> {
        println!("hello world from geodataframe!");

        Ok(())
    }

    // fn area(&self) -> Result<Series> {
    //     // Connect to geo's area function

    //     todo!()
    // }
}
