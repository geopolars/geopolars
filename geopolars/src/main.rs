use geopolars::error::Result;
// use geopolars::geodataframe::GeoDataFrame;
use geopolars::geoseries::GeoSeries;
use polars::prelude::{IpcReader, SerReader};
use std::fs::File;
use std::time::Instant;
use polars::export::arrow::array::ListArray;
use geo::arrow::polygon::polygon_index;
use geo::area::area_polygon;

fn main() -> Result<()> {
    let file = File::open("datasets/ne_10m_admin_1_states_provinces.arrow").expect("file not found");

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
    // println!("{}", df);

    let geometry = df.column("geometry").unwrap();
    // let area = geometry.area().unwrap();
    let chunk = &geometry.0.chunks()[0];
    let struct_arrow_array = chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap();
    let scalar = polygon_index(struct_arrow_array, 0).unwrap();
    let value = area_polygon(&scalar);

    println!("{:?}", value);

    // let x = df.column("geometry")?.exterior()?;
    // println!("{}", x);

    // let start = Instant::now();
    // let _ = df.centroid()?;
    // let _ = df.column("geometry")?.centroid()?;
    // println!("Debug: {}", start.elapsed().as_secs_f32());

    Ok(())
}
