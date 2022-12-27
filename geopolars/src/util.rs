use geo::{Coord, Geometry, LineString, Point, Polygon};
use geozero::{wkb::Wkb, ToGeo};
use polars::datatypes::{AnyValue, DataType};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::num;
use polars::prelude::{PolarsError, PolarsResult, Series};

/// Helper function to iterate over geometries from polars Series
pub(crate) fn iter_geom(series: &Series) -> impl Iterator<Item = Geometry<f64>> + '_ {
    let chunks = series.binary().expect("series was not a list type");

    let iter = chunks.into_iter();
    iter.map(|row| {
        let value = row.expect("Row is null");
        Wkb(value.to_vec())
            .to_geo()
            .expect("unable to convert to geo")
    })
}

/// Access to a geometry at a specified index
pub fn geom_at_index(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    // let struct_type = DataType::Struct(vec![X_FIELD, Y_FIELD]);
    match series.dtype() {
        DataType::Binary => geom_at_index_wkb(series, index),
        DataType::Struct(_) => geom_at_index_point(series, index),
        DataType::List(dt) => match *dt.clone() {
            DataType::Struct(_) => geom_at_index_linestring(series, index),
            DataType::List(_) => geom_at_index_polygon(series, index),
            _ => unimplemented!(),
        },

        _ => unimplemented!(),
    }
}

fn geom_at_index_wkb(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let buffer = match series.get(index) {
        Ok(AnyValue::Binary(buf)) => buf,
        _ => return Err(PolarsError::SchemaMisMatch("".into())),
    };

    let geom = Wkb(buffer.to_vec())
        .to_geo()
        .expect("unable to convert geo");
    Ok(geom)
}

/// Access geo point out of geoarrow point column at given index
fn geom_at_index_point(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let (chunk, local_index) = get_chunk_and_local_index(series, index);

    let struct_array = chunk.as_any().downcast_ref::<StructArray>().unwrap();

    let struct_array_values = struct_array.values();
    let x_arrow_array = &struct_array_values[0];
    let y_arrow_array = &struct_array_values[1];

    let x_array_values = x_arrow_array
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    let y_array_values = y_arrow_array
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();

    let p = Point::new(
        x_array_values.value(local_index),
        y_array_values.value(local_index),
    );
    Ok(Geometry::Point(p))
}

/// Access a single LineString out of a GeoArrow LineString column
fn geom_at_index_linestring(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let (chunk, local_index) = get_chunk_and_local_index(series, index);

    let list_array = chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap();
    let inner_dyn_array = list_array.value(local_index);

    let struct_array = inner_dyn_array
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let l = parse_linestring(struct_array)?;
    Ok(Geometry::LineString(l))
}

/// Parse a slice of a list array into a geo LineString
/// The slice is expected to be a StructArray with two children, x and y
fn parse_linestring(linestring: &StructArray) -> PolarsResult<LineString<f64>> {
    let struct_array_values = linestring.values();
    let x_arrow_array = &struct_array_values[0];
    let y_arrow_array = &struct_array_values[1];

    let x_array_values = x_arrow_array
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();
    let y_array_values = y_arrow_array
        .as_any()
        .downcast_ref::<PrimitiveArray<f64>>()
        .unwrap();

    let mut coords: Vec<Coord> = Vec::with_capacity(x_array_values.len());
    for i in 0..x_array_values.len() {
        coords.push(Coord {
            x: x_array_values.value(i),
            y: y_array_values.value(i),
        })
    }

    Ok(LineString::new(coords))
}

fn geom_at_index_polygon(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let (geometry_dyn_array, local_index) = get_chunk_and_local_index(series, index);

    let geometry_array = geometry_dyn_array
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap();

    let ring_dyn_array = geometry_array.value(local_index);
    let ring_array = ring_dyn_array
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap();

    let exterior_ring_dyn = ring_array.value(0);
    let exterior_ring = exterior_ring_dyn
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let exterior_linestring = parse_linestring(exterior_ring)?;

    let mut interior_rings: Vec<LineString<f64>> = Vec::with_capacity(ring_array.len() - 1);
    for ring_index in 1..ring_array.len() {
        let interior_ring_dyn = ring_array.value(ring_index);
        let interior_ring = interior_ring_dyn
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        interior_rings.push(parse_linestring(interior_ring)?);
    }

    let p = Polygon::new(exterior_linestring, interior_rings);
    Ok(Geometry::Polygon(p))
}

/// Returns underlying arrow2 array containing the current item plus the local index within that
/// array
fn get_chunk_and_local_index(series: &Series, global_index: usize) -> (Box<dyn Array>, usize) {
    // Index of underlying chunk in Series
    let mut chunk_index: usize = 0;

    // Counter for sum of sizes of previous chunks
    let mut acc: usize = 0;

    for chunk_len in series.chunk_lengths() {
        if global_index < acc + chunk_len {
            continue;
        }
        chunk_index += 1;
        acc += chunk_len;
    }

    // I _think_ this clone is light because it's behind a box?
    let chunk = series.chunks()[chunk_index].clone();
    (chunk, global_index - acc)
}

pub enum Predicate {
    Intersects,
    Contains,
}

/// Get the index of the chunk and the index of the value in that chunk
// From: https://github.com/pola-rs/polars/blob/f8bb5aaa9bb8f8c3c9365933a062758478fb63ad/polars/polars-core/src/chunked_array/ops/downcast.rs#L76-L83
#[inline]
pub(crate) fn index_to_chunked_index(series: &Series, index: usize) -> (usize, usize) {
    if series.chunks().len() == 1 {
        return (0, index);
    }

    _index_to_chunked_index(series.chunk_lengths(), index)
}

/// This logic is same as the impl on ChunkedArray
/// The difference is that there is less indirection because the caller should preallocate
/// `chunk_lens` once. On the `ChunkedArray` we indirect through an `ArrayRef` which is an indirection
/// and a vtable.
// From: https://github.com/pola-rs/polars/blob/f8bb5aaa9bb8f8c3c9365933a062758478fb63ad/polars/polars-core/src/utils/mod.rs#L822-L846
#[inline]
pub(crate) fn _index_to_chunked_index<
    I: Iterator<Item = Idx>,
    Idx: PartialOrd + std::ops::AddAssign + std::ops::SubAssign + num::Zero + num::One,
>(
    chunk_lens: I,
    index: Idx,
) -> (Idx, Idx) {
    let mut index_remainder = index;
    let mut current_chunk_idx = num::Zero::zero();

    for chunk_len in chunk_lens {
        if chunk_len > index_remainder {
            break;
        } else {
            index_remainder -= chunk_len;
            current_chunk_idx += num::One::one();
        }
    }
    (current_chunk_idx, index_remainder)
}
