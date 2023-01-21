use crate::geo_traits::MultiLineStringTrait;
use crate::LineString;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;

/// An arrow equivalent of a Polygon
#[derive(Debug, Clone)]
pub struct MultiLineString<'a> {
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

impl<'a> MultiLineStringTrait<'a> for MultiLineString<'a> {
    type ItemType = LineString<'a>;

    fn num_lines(&'a self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn line(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        Some(LineString {
            x: self.x,
            y: self.y,
            geom_offsets: self.ring_offsets,
            geom_index: start + i,
        })
    }
}
