use polars::prelude::{Result, DataFrame, Series};

pub trait GeoDataFrame {
    fn hello_world(&self) -> Result<()>;

    // fn area(&self) -> Result<Series>;
}

impl GeoDataFrame for DataFrame {
    fn hello_world(&self) -> Result<()> {
        println!("hello world from geodataframe!");

        Ok(())
    }

    // fn area(&self) -> Result<Series> {
    //     // Connect to geo's area function


    //     todo!()
    // }
}
