use arctic::geodataframe::GeoDataFrame;
use polars::prelude::{DataFrame, IpcReader, SerReader, Result};
use std::fs::File;

fn main() -> Result<()> {
    let file = File::open("cities.arrow").expect("file not found");
    let df = IpcReader::new(file).finish()?;
    println!("{}", df);

    df.centroid();

    // let df = DataFrame::default();
    // df.hello_world();

    // println!("hello world from main!");

    Ok(())
}
