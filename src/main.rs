use arctic::geodataframe::GeoDataFrame;
use arrow2::io::ipc::{
    read::{read_file_metadata, FileReader},
    write::{FileWriter, WriteOptions},
};
use polars::prelude::{DataFrame, IpcReader, Result, SerReader};
use std::fs::File;

fn main() -> Result<()> {
    let mut file = File::open("cities.arrow").expect("file not found");

    // let metadata = read_file_metadata(&mut reader)?;
    // let mut filereader = FileReader::new(reader, metadata.clone(), None);
    // for chunk in filereader {
    //     let chunk = chunk?;
    //     for col in chunk.columns() {
    //         col.
    //     }
    //     println!("{:#?}", chunk);
    //     let df = DataFrame::try_from((chunk, metadata.schema.fields.as_ref())).unwrap();
    //     println!("{}", df);
    //     println!("{:#?}", df.schema());
    // }

    // println!("{:#?}", metadata);

    let df = IpcReader::new(file).finish()?;
    println!("{}", df);

    df.centroid();

    // let df = DataFrame::default();
    // df.hello_world();

    // println!("hello world from main!");

    Ok(())
}
