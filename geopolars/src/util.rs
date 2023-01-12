use crate::error::Result;
use geo::{Coord, Geometry, LineString, Point, Polygon};
use geozero::{wkb::Wkb, ToGeo};
use geozero::{CoordDimensions, ToWkb};
use polars::datatypes::{AnyValue, DataType};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::export::arrow::array::{ListArray, PrimitiveArray, StructArray};
use polars::export::num;
use polars::prelude::{PolarsError, PolarsResult, Series};
use std::convert::Into;

pub fn from_geom_vec(geoms: &[Geometry<f64>]) -> Result<Series> {
    let mut wkb_array = MutableBinaryArray::<i32>::with_capacity(geoms.len());

    for geom in geoms {
        let wkb = geom.to_wkb(CoordDimensions::xy()).map_err(|_| {
            PolarsError::ComputeError(ErrString::from("Failed to convert geom vec to GeoSeries"))
        })?;
        wkb_array.push(Some(wkb));
    }
    let array: BinaryArray<i32> = wkb_array.into();

    let series = Series::try_from(("geometry", Box::new(array) as Box<dyn Array>))?;
    Ok(series)
}

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
    let (chunk_idx, local_idx) = index_to_chunked_index(series, index);

    let struct_array = series.chunks()[chunk_idx]
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

    let p = Point::new(
        x_array_values.value(local_idx),
        y_array_values.value(local_idx),
    );
    Ok(Geometry::Point(p))
}

/// Access a single LineString out of a GeoArrow LineString column
fn geom_at_index_linestring(series: &Series, index: usize) -> PolarsResult<Geometry<f64>> {
    let (chunk_idx, local_idx) = index_to_chunked_index(series, index);

    let list_array = series.chunks()[chunk_idx]
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap();
    let inner_dyn_array = list_array.value(local_idx);

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
    let (chunk_idx, local_idx) = index_to_chunked_index(series, index);
    let geometry_dyn_array = &series.chunks()[chunk_idx];

    let geometry_array = geometry_dyn_array
        .as_any()
        .downcast_ref::<ListArray<i64>>()
        .unwrap();

    let ring_dyn_array = geometry_array.value(local_idx);
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

pub enum Predicate {
    Intersects,
    Contains,
}
