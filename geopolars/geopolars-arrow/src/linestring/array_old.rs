use geo::{Coord, LineString};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::utils::{BitmapIter, ZipValidity};
use polars::export::arrow::bitmap::Bitmap;
use polars::export::arrow::buffer::Buffer;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::Series;

use crate::util::index_to_chunked_index;

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct LineStringSeries<'a>(pub &'a Series);

impl LineStringSeries<'_> {
    pub fn get(&self, i: usize) -> Option<LineStringScalar> {
        let (chunk_idx, local_idx) = index_to_chunked_index(self.0, i);
        let chunk = &self.0.chunks()[chunk_idx];

        let linestring_array =
            LineStringArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap());
        linestring_array.get(local_idx)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        let line_string_item = self.get(i);
        line_string_item.map(|ls| ls.into_geo())
    }

    pub fn chunks(&self) -> Vec<LineStringArray> {
        self.0
            .chunks()
            .iter()
            .map(|chunk| LineStringArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap()))
            .collect()
    }
}
