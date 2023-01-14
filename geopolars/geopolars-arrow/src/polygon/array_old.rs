use geo::{Coord, LineString, Polygon};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::utils::{BitmapIter, ZipValidity};
use polars::export::arrow::bitmap::Bitmap;
use polars::export::arrow::buffer::Buffer;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::Series;

use crate::linestring::LineStringScalar;
use crate::util::index_to_chunked_index;

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PolygonSeries<'a>(pub &'a Series);

impl PolygonSeries<'_> {
    pub fn get(&self, i: usize) -> Option<PolygonScalar> {
        let (chunk_idx, local_idx) = index_to_chunked_index(self.0, i);
        let chunk = &self.0.chunks()[chunk_idx];

        let polygon_array = PolygonArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap());
        polygon_array.get(local_idx)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        let polygon_item = self.get(i);
        polygon_item.map(|p| p.into_geo())
    }

    pub fn chunks(&self) -> Vec<PolygonArray> {
        self.0
            .chunks()
            .iter()
            .map(|chunk| PolygonArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap()))
            .collect()
    }
}
