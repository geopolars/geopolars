use crate::algorithm::bounding_rect::bounding_rect_multilinestring;
use crate::geo_traits::MultiLineStringTrait;
use crate::LineString;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;
use rstar::{RTreeObject, AABB};

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

impl From<MultiLineString<'_>> for geo::MultiLineString {
    fn from(value: MultiLineString<'_>) -> Self {
        (&value).into()
    }
}

impl From<&MultiLineString<'_>> for geo::MultiLineString {
    fn from(value: &MultiLineString<'_>) -> Self {
        // Start and end indices into the ring_offsets buffer
        let (start_geom_idx, end_geom_idx) = value.geom_offsets.start_end(value.geom_index);

        let mut line_strings: Vec<geo::LineString> =
            Vec::with_capacity(end_geom_idx - start_geom_idx);

        for ring_idx in start_geom_idx..end_geom_idx {
            let (start_coord_idx, end_coord_idx) = value.ring_offsets.start_end(ring_idx);
            let mut ring: Vec<geo::Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
            for coord_idx in start_coord_idx..end_coord_idx {
                ring.push(geo::Coord {
                    x: value.x[coord_idx],
                    y: value.y[coord_idx],
                })
            }
            line_strings.push(ring.into());
        }

        geo::MultiLineString::new(line_strings)
    }
}

impl From<MultiLineString<'_>> for geo::Geometry {
    fn from(value: MultiLineString<'_>) -> Self {
        geo::Geometry::MultiLineString(value.into())
    }
}

impl RTreeObject for MultiLineString<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multilinestring(self);
        AABB::from_corners(lower, upper)
    }
}
