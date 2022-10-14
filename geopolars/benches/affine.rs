use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use geopolars::geoseries::GeoSeries;
use geopolars::geoseries::TransformOrigin;
use polars::prelude::*;
use polars::prelude::{IpcReader, PolarsResult, SerReader};
use std::fs::File;

fn load_data() -> PolarsResult<Series> {
    // Assuming current dir is /geopolars/
    let file = File::open("../data/cities.arrow").expect("file not found");

    let df = IpcReader::new(file).finish()?;
    df.column("geometry").cloned()
}

fn bench_translate(b: &mut Bencher) {
    let series = load_data().expect("Unable to load series");
    b.iter(|| series.translate(10.0, 10.0))
}

fn bench_scale(b: &mut Bencher) {
    let series = load_data().expect("Unable to load series");
    b.iter(|| series.scale(2.0, 2.0, TransformOrigin::Centroid));
}

fn affine_parsing_benchmark(c: &mut Criterion) {
    c.bench_function("translate", bench_translate);
    c.bench_function("scale", bench_scale);
}

criterion_group!(benches, affine_parsing_benchmark);
criterion_main!(benches);
