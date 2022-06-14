use arrow2::array::{self, ListArray};
use arrow2::datatypes::{DataType, Field};
use geopolars::geodataframe::GeoDataFrame;
use geopolars::geoseries::GeoSeries;
use polars::prelude::{IpcReader, Result, SerReader, Series};
use std::fs::File;
use std::time::Instant;

fn main() -> Result<()> {
    let inner = Box::new(Field::new("geoarrow.coord", DataType::UInt8, false));
    let extension_type = DataType::Extension(
        "geoarrow.wkb".to_string(),
        Box::new(DataType::List(inner)),
        Some("metadata".to_string()),
    );

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

    let geom_column = df.column("geometry")?;
    let list_array = geom_column.rechunk().list()?;
    let chunk = list_array.chunks().first().unwrap();
    let out = chunk.as_any().downcast_ref::<ListArray<i32>>().unwrap();
    out.t

    // let x = chunk.as_any();
    chunk.
    let a = geom_column.list()?;
    a.
    // geom_column.list()
    // df.co

    let x = df.column("geometry")?.exterior()?;
    println!("{}", x);

    let start = Instant::now();
    let _ = df.centroid()?;
    let _ = df.column("geometry")?.centroid()?;
    println!("Debug: {}", start.elapsed().as_secs_f32());

    Ok(())
}
