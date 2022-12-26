use geo::{Coord, Geometry, LineString, Point};
use geozero::{wkb::Wkb, ToGeo};
use polars::datatypes::{AnyValue, DataType};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
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

fn geom_at_index_linestring(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let (chunk, local_index) = get_chunk_and_local_index(series, index);

    let list_array = chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap();
    let inner_dyn_array = list_array.value(local_index);

    let struct_array = inner_dyn_array
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

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

    let mut coords: Vec<Coord> = Vec::with_capacity(x_array_values.len());
    for i in 0..x_array_values.len() {
        coords.push(Coord {
            x: x_array_values.value(i),
            y: y_array_values.value(i),
        })
    }

    let l = LineString::new(coords);
    Ok(Geometry::LineString(l))
}

fn geom_at_index_polygon(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    todo!()
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
