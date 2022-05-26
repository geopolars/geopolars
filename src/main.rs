use arctic::geodataframe::GeoDataFrame;
use polars::prelude::DataFrame;

fn main() {
    let df = DataFrame::default();
    df.hello_world();

    println!("hello world from main!");
}
