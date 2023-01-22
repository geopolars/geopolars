use crate::algorithm::bounding_rect::bounding_rect_polygon;
use crate::geo_traits::PolygonTrait;
use crate::LineString;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;
use rstar::{RTreeObject, AABB};

use super::iterator::PolygonInteriorIterator;

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
    type Iter = PolygonInteriorIterator<'a>;

    fn exterior(&'a self) -> Self::ItemType {
        let (start, _) = self.geom_offsets.start_end(self.geom_index);
        LineString {
            x: self.x,
            y: self.y,
            geom_offsets: self.ring_offsets,
            geom_index: start,
        }
    }

    fn interiors(&'a self) -> Self::Iter {
        PolygonInteriorIterator::new(self)
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

impl From<Polygon<'_>> for geo::Polygon {
    fn from(value: Polygon<'_>) -> Self {
        (&value).into()
    }
}

impl From<&Polygon<'_>> for geo::Polygon {
    fn from(value: &Polygon<'_>) -> Self {
        super::parse_polygon(
            value.x,
            value.y,
            value.geom_offsets,
            value.ring_offsets,
            value.geom_index,
        )
    }
}

impl From<Polygon<'_>> for geo::Geometry {
    fn from(value: Polygon<'_>) -> Self {
        geo::Geometry::Polygon(value.into())
    }
}

impl RTreeObject for Polygon<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_polygon(self);
        AABB::from_corners(lower, upper)
    }
}
