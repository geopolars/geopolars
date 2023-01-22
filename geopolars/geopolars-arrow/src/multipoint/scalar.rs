use crate::algorithm::bounding_rect::bounding_rect_multipoint;
use crate::geo_traits::MultiPointTrait;
use crate::Point;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;
use rstar::{RTreeObject, AABB};
use std::slice::Iter;

/// An arrow equivalent of a MultiPoint
#[derive(Debug, Clone)]
pub struct MultiPoint<'a> {
    /// Buffer of x coordinates
    pub x: &'a Buffer<f64>,

    /// Buffer of y coordinates
    pub y: &'a Buffer<f64>,

    /// Offsets into the coordinate array where each geometry starts
    pub geom_offsets: &'a OffsetsBuffer<i64>,

    pub geom_index: usize,
}

impl<'a> MultiPointTrait<'a> for MultiPoint<'a> {
    type ItemType = Point<'a>;
    type Iter = Iter<'a, Self::ItemType>;

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

impl From<MultiPoint<'_>> for geo::MultiPoint {
    fn from(value: MultiPoint<'_>) -> Self {
        (&value).into()
    }
}

impl From<&MultiPoint<'_>> for geo::MultiPoint {
    fn from(value: &MultiPoint<'_>) -> Self {
        let (start_idx, end_idx) = value.geom_offsets.start_end(value.geom_index);
        let mut coords: Vec<geo::Point> = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            coords.push(geo::Point::new(value.x[i], value.y[i]))
        }

        geo::MultiPoint::new(coords)
    }
}

impl From<MultiPoint<'_>> for geo::Geometry {
    fn from(value: MultiPoint<'_>) -> Self {
        geo::Geometry::MultiPoint(value.into())
    }
}

impl RTreeObject for MultiPoint<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipoint(self);
        AABB::from_corners(lower, upper)
    }
}
