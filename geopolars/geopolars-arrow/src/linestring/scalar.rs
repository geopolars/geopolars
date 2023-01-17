use crate::geo_traits::LineStringTrait;
use crate::Point;
use polars::export::arrow::buffer::Buffer;
use polars::export::arrow::offset::OffsetsBuffer;

/// An arrow equivalent of a LineString
#[derive(Debug, Clone)]
pub struct LineString<'a> {
    /// Buffer of x coordinates
    pub x: &'a Buffer<f64>,

    /// Buffer of y coordinates
    pub y: &'a Buffer<f64>,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<i64>,

    pub geom_index: usize,
}

impl<'a> LineStringTrait<'a> for LineString<'a> {
    type ItemType = Point<'a>;

    // Don't know how to return an iterator over these point objects
    // https://stackoverflow.com/a/27535594
    // fn points(&'a self) -> Self::Iter {
    //     (0..self.num_points()).into_iter().map(|i| self.point(i).unwrap())
    // }

    fn num_points(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }

        let point = Point {
            x: self.x,
            y: self.y,
            geom_index: start + i,
        };
        Some(point)
    }
}
