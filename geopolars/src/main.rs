use geopolars::error::Result;
use geopolars::geodataframe::GeoDataFrame;
use geopolars::geoseries::GeoSeries;
use polars::prelude::{IpcReader, SerReader};
use std::fs::File;
use std::time::Instant;

fn main() -> Result<()> {
    let file = File::open("cities.arrow").expect("file not found");

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

    let x = df.column("geometry")?.exterior()?;
    println!("{}", x);

    let start = Instant::now();
    let _ = df.centroid()?;
    let _ = df.column("geometry")?.centroid()?;
    println!("Debug: {}", start.elapsed().as_secs_f32());

    Ok(())
}
