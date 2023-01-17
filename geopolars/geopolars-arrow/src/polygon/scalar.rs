use crate::geo_traits::PolygonTrait;
use crate::LineString;
use polars::export::arrow::buffer::Buffer;
use polars::export::arrow::offset::OffsetsBuffer;

/// An arrow equivalent of a Polygon
#[derive(Debug, Clone)]
pub struct Polygon<'a> {
    /// Buffer of x coordinates
    pub x: &'a Buffer<f64>,

    /// Buffer of y coordinates
    pub y: &'a Buffer<f64>,

    /// Offsets into the ring array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: &'a OffsetsBuffer<i64>,

    pub geom_index: usize,
}

impl<'a> PolygonTrait<'a> for Polygon<'a> {
    type ItemType = LineString<'a>;

    fn exterior(&'a self) -> Self::ItemType {
        let (start, _) = self.geom_offsets.start_end(self.geom_index);
        LineString {
            x: self.x,
            y: self.y,
            geom_offsets: self.ring_offsets,
            geom_index: start,
        }
    }

    fn num_interiors(&'a self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start - 1
    }

    fn interior(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start - 1) {
            return None;
        }

        Some(LineString {
            x: self.x,
            y: self.y,
            geom_offsets: self.ring_offsets,
            geom_index: start + 1 + i,
        })
    }
}
