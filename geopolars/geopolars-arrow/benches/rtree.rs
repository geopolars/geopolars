use arrow2::array::BinaryArray;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use geo::BoundingRect;
use geopolars_arrow::{GeometryArrayTrait, MultiPolygonArray};
use geozero::wkb::Wkb;
use geozero::ToGeo;
use rstar::{RTree, RTreeObject, AABB};
use std::io::Cursor;

fn load_data() -> MultiPolygonArray {
    let path = "nz-building-outlines.parquet";
    let data = std::fs::read(path).expect("Unable to read file");
    let mut input_file = Cursor::new(data.as_slice());

    let metadata = read_metadata(&mut input_file).unwrap();
    let schema = infer_schema(&metadata).unwrap();

    let schema = schema.filter(|index, _field| index == 13);

    let mut file_reader = FileReader::new(
        input_file,
        metadata.row_groups,
        schema.clone(),
        None,
        None,
        None,
    );

    // This file has one row group
    let chunk = file_reader.next().unwrap().unwrap();

    // Simple checks to verify the file hasn't changed
    assert_eq!(chunk.columns().len(), 1, "one column expected");
    assert_eq!(chunk.len(), 3320498);

    let box_dyn_arr = &chunk.columns()[0];
    let arr = box_dyn_arr
        .as_any()
        .downcast_ref::<BinaryArray<i32>>()
        .unwrap()
        .clone();

    let mut multipolygon_vec: Vec<geo::MultiPolygon> = vec![];
    for item in arr.into_iter().flatten() {
        let geom = Wkb(item.to_vec()).to_geo().unwrap();
        match geom {
            geo::Geometry::Polygon(g) => multipolygon_vec.push(geo::MultiPolygon::new(vec![g])),
            geo::Geometry::MultiPolygon(g) => multipolygon_vec.push(g),
            _ => panic!("unexpected type"),
        }
    }

    multipolygon_vec.into()
}

struct MyMultiPolygon<'a>(&'a geo::MultiPolygon);

impl<'a> RTreeObject for MyMultiPolygon<'a> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let rect = self.0.bounding_rect().unwrap();
        let lower: [f64; 2] = rect.min().into();
        let upper: [f64; 2] = rect.max().into();
        AABB::from_corners(lower, upper)
    }
}

fn create_rtree_from_geo_vec(geo_vec: &Vec<geo::MultiPolygon>) -> RTree<MyMultiPolygon> {
    let mut tree = RTree::new();
    for geom in geo_vec {
        tree.insert(MyMultiPolygon(&geom));
    }

    tree
}

fn bench_rtree_from_geoarrow(b: &mut Bencher) {
    let arr = load_data();
    b.iter(|| arr.rstar_tree());
}

fn bench_rtree_from_geo(b: &mut Bencher) {
    let geo_vec: Vec<geo::MultiPolygon> = load_data().iter_geo_values().collect();
    b.iter(|| create_rtree_from_geo_vec(&geo_vec));
}

fn benchmark_group(c: &mut Criterion) {
    c.bench_function("rtree_from_geoarrow", bench_rtree_from_geoarrow);
    c.bench_function("rtree_from_geo", bench_rtree_from_geo);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_group
}
criterion_main!(benches);
