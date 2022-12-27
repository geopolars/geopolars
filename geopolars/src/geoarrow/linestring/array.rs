// use crate::traits::line_string::LineStringTrait;
use geo::{Coord, LineString};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::prelude::Series;

use crate::util::index_to_chunked_index;

/// A struct representing a non-null single LineString geometry
#[derive(Debug, Clone)]
pub struct LineStringScalar(StructArray);

impl LineStringScalar {
    pub fn into_geo(self) -> LineString {
        let struct_array_values = self.0.values();
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

        LineString::new(coords)
    }
}

#[derive(Debug, Clone)]
pub struct LineStringArray(ListArray<i64>);

impl LineStringArray {
    pub fn get(&self, i: usize) -> Option<LineStringScalar> {
        if self.0.is_null(i) {
            return None;
        }

        let line_string_item = self
            .0
            .value(i)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        Some(LineStringScalar(line_string_item.clone()))
    }

    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        let line_string_item = self.get(i);

        if let Some(line_string_item) = line_string_item {
            Some(line_string_item.into_geo())
        } else {
            return None;
        }
    }
}

#[derive(Debug, Clone)]
pub struct LineStringSeries(Series);

impl LineStringSeries {
    pub fn get(&self, i: usize) -> Option<LineStringScalar> {
        let (chunk_idx, local_idx) = index_to_chunked_index(&self.0, i);
        let chunk = self.0.chunks()[chunk_idx];

        let linestring_array = LineStringArray(
            chunk
                .as_any()
                .downcast_ref::<ListArray<i64>>()
                .unwrap()
                .clone(),
        );
        linestring_array.get(local_idx)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        let line_string_item = self.get(i);

        if let Some(line_string_item) = line_string_item {
            Some(line_string_item.into_geo())
        } else {
            return None;
        }
    }
}
