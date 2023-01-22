use crate::algorithm::bounding_rect::bounding_rect_multipolygon;
use crate::geo_traits::MultiPolygonTrait;
use crate::Polygon;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;
use rstar::{RTreeObject, AABB};

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

impl From<MultiPolygon<'_>> for geo::MultiPolygon {
    fn from(value: MultiPolygon<'_>) -> Self {
        (&value).into()
    }
}

impl From<&MultiPolygon<'_>> for geo::MultiPolygon {
    fn from(value: &MultiPolygon<'_>) -> Self {
        // Start and end indices into the polygon_offsets buffer
        let (start_geom_idx, end_geom_idx) = value.geom_offsets.start_end(value.geom_index);

        let mut polygons: Vec<geo::Polygon> = Vec::with_capacity(end_geom_idx - start_geom_idx);

        for geom_idx in start_geom_idx..end_geom_idx {
            let poly = crate::polygon::util::parse_polygon(
                value.x,
                value.y,
                value.polygon_offsets,
                value.ring_offsets,
                geom_idx,
            );
            polygons.push(poly);
        }

        geo::MultiPolygon::new(polygons)
    }
}

impl From<MultiPolygon<'_>> for geo::Geometry {
    fn from(value: MultiPolygon<'_>) -> Self {
        geo::Geometry::MultiPolygon(value.into())
    }
}

impl RTreeObject for MultiPolygon<'_> {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        let (lower, upper) = bounding_rect_multipolygon(self);
        AABB::from_corners(lower, upper)
    }
}
