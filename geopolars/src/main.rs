use geopolars::error::Result;
use geopolars::geodataframe::GeoDataFrame;
use geopolars::geoseries::GeoSeries;
use geopolars::util::geom_at_index;
use polars::prelude::{IpcReader, SerReader};
use std::fs::File;
use std::time::Instant;

fn main() -> Result<()> {
    // let file = File::open("../py-geopolars/python/geopolars/datasets/ne_10m_railroads.arrow").expect("file not found");
    // let file =
    //     File::open("../py-geopolars/python/geopolars/datasets/ne_110m_glaciated_areas.arrow")
    //         .expect("file not found");

    // let file =
    //     File::open("../py-geopolars/python/geopolars/datasets/naturalearth_cities.arrow")
    //         .expect("file not found");
    let file =
        File::open("../py-geopolars/python/geopolars/datasets/naturalearth_cities_struct.arrow")
            .expect("file not found");

    let df = IpcReader::new(file).finish()?;
    println!("{}", df);

    let series = df.column("geometry")?;
    println!("{}", series);

    let first_geom = geom_at_index(series, 0)?;
    println!("{:?}", first_geom);

    let start = Instant::now();
    let x_vals = series.x()?;
    println!("{:?}", x_vals);
    println!("Debug: {}", start.elapsed().as_secs_f32());

    // let start = Instant::now();
    // let _ = df.centroid()?;
    // let _ = df.column("geometry")?.centroid()?;
    // println!("Debug: {}", start.elapsed().as_secs_f32());

    Ok(())
}
