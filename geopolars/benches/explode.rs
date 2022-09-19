use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use geo::Geometry;
use geo::MultiPoint;
use geo::Point;
use geopolars::error::Result;
use geopolars::geoseries::GeoSeries;
use polars::prelude::*;

fn generate_multipoint_series() -> Result<Series> {
    let points: Vec<Point> = (0..90_000).map(|_| Point::new(0., 0.)).collect();
    let multipoints: Vec<Geometry> = points
        .chunks(2)
        .map(|points| MultiPoint::new(points.to_vec()))
        .map(Geometry::MultiPoint)
        .collect();
    let series = Series::from_geom_vec(&multipoints).unwrap();
    Ok(series)
}

fn bench_explode(b: &mut Bencher) {
    let series = generate_multipoint_series().expect("Unable to generate multipoint series");
    b.iter(|| GeoSeries::explode(&series))
}

fn explode_benchmark(c: &mut Criterion) {
    c.bench_function("explode", bench_explode);
}

criterion_group!(benches, explode_benchmark);
criterion_main!(benches);
