use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use geopolars_geo::geoseries::GeoSeries;
use geopolars_geo::ops::affine::TransformOrigin;
use polars::prelude::*;
use polars::prelude::{IpcReader, PolarsResult, SerReader};
use std::fs::File;

fn load_data() -> PolarsResult<Series> {
    // Assuming current dir is /geopolars/
    let file = File::open("../data/cities.arrow").expect("file not found");

    let df = IpcReader::new(file).memory_mapped(false).finish()?;
    df.column("geometry").cloned()
}

fn load_struct_data() -> PolarsResult<Series> {
    // Assuming current dir is /geopolars/
    let file = File::open("../data/cities_struct.arrow").expect("file not found");

    let df = IpcReader::new(file).memory_mapped(false).finish()?;
    df.column("geometry").cloned()
}

fn bench_translate(b: &mut Bencher) {
    let series = load_data().expect("Unable to load series");
    b.iter(|| series.translate(10.0, 10.0))
}

fn bench_translate_geoarrow(b: &mut Bencher) {
    let series = load_struct_data().expect("Unable to load series");
    b.iter(|| series.translate(10.0, 10.0))
}

fn bench_scale(b: &mut Bencher) {
    let series = load_data().expect("Unable to load series");
    b.iter(|| series.scale(2.0, 2.0, TransformOrigin::Centroid));
}

fn affine_parsing_benchmark(c: &mut Criterion) {
    c.bench_function("translate", bench_translate);
    c.bench_function("translate geoarrow", bench_translate_geoarrow);
    c.bench_function("scale", bench_scale);
}

criterion_group!(benches, affine_parsing_benchmark);
criterion_main!(benches);
