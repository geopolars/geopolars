use crate::geo_traits::MultiPolygonTrait;
use crate::Polygon;
use polars::export::arrow::buffer::Buffer;
use polars::export::arrow::offset::OffsetsBuffer;

/// An arrow equivalent of a Polygon
#[derive(Debug, Clone)]
pub struct MultiPolygon<'a> {
    /// Buffer of x coordinates
    pub x: &'a Buffer<f64>,

    /// Buffer of y coordinates
    pub y: &'a Buffer<f64>,

    /// Offsets into the polygon array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<i64>,

    /// Offsets into the ring array where each polygon starts
    pub polygon_offsets: &'a OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    pub ring_offsets: &'a OffsetsBuffer<i64>,

    pub geom_index: usize,
}

impl<'a> MultiPolygonTrait<'a> for MultiPolygon<'a> {
    type ItemType = Polygon<'a>;

    fn num_polygons(&'a self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn polygon(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        // TODO: double check offsets is correct
        Some(Polygon {
            x: self.x,
            y: self.y,
            geom_offsets: self.polygon_offsets,
            ring_offsets: self.ring_offsets,
            geom_index: start + i,
        })
    }
}
