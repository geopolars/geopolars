use geo::{Coord, LineString, Polygon};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::utils::{BitmapIter, ZipValidity};
use polars::export::arrow::bitmap::Bitmap;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::Series;

use crate::linestring::array::LineStringScalar;
use crate::util::index_to_chunked_index;

/// A struct representing a non-null single LineString geometry
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PolygonScalar(ListArray<i64>);

impl PolygonScalar {
    pub fn into_geo(self) -> Polygon {
        let exterior_value = self.0.value(0);
        let exterior_ring = exterior_value
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let exterior_linestring = LineStringScalar(exterior_ring.clone()).into_geo();

        let has_interior_rings = self.0.len() > 1;
        let n_interior_rings = has_interior_rings.then(|| self.0.len() - 2).unwrap_or(0);

        let mut interior_rings: Vec<LineString<f64>> = Vec::with_capacity(n_interior_rings);
        for i in 0..n_interior_rings {
            let interior_value = self.0.value(i + 1);
            let interior_ring = interior_value
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            interior_rings.push(LineStringScalar(interior_ring.clone()).into_geo());
        }

        Polygon::new(exterior_linestring, interior_rings)
    }
}

#[derive(Debug, Clone)]
pub struct PolygonArrayParts<'a> {
    pub x: &'a PrimitiveArray<f64>,
    pub y: &'a PrimitiveArray<f64>,
    pub ring_offsets: &'a OffsetsBuffer<i64>,
    pub geom_offsets: &'a OffsetsBuffer<i64>,
    pub validity: Option<&'a Bitmap>,
}

impl<'a> PolygonArrayParts<'a> {
    pub fn len(&self) -> usize {
        self.geom_offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn values_iter_coords(&self) -> impl Iterator<Item = Coord> + '_ {
        self.x
            .values_iter()
            .zip(self.y.values_iter())
            .map(|(x, y)| Coord { x: *x, y: *y })
    }

    pub fn iter_coords(&self) -> ZipValidity<Coord, impl Iterator<Item = Coord> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter_coords(), self.validity)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        let is_null = self.validity.map(|x| !x.get_bit(i)).unwrap_or(false);
        if is_null {
            return None;
        }

        // Start and end indices into the ring_offsets buffer
        let (start_geom_idx, end_geom_idx) = self.geom_offsets.start_end(i);

        // Parse exterior ring first
        let (start_ext_ring_idx, end_ext_ring_idx) = self.ring_offsets.start_end(start_geom_idx);
        let mut exterior_coords: Vec<Coord> =
            Vec::with_capacity(end_ext_ring_idx - start_ext_ring_idx);

        for i in start_ext_ring_idx..end_ext_ring_idx {
            exterior_coords.push(Coord {
                x: self.x.value(i),
                y: self.y.value(i),
            })
        }
        let exterior_ring: LineString = exterior_coords.into();

        // Parse any interior rings
        // Note: need to check if interior rings exist otherwise the subtraction below can overflow
        let has_interior_rings = end_geom_idx - start_geom_idx > 1;
        let n_interior_rings = if has_interior_rings {
            end_geom_idx - start_geom_idx - 2
        } else {
            0
        };
        let mut interior_rings: Vec<LineString<f64>> = Vec::with_capacity(n_interior_rings);
        for ring_idx in start_geom_idx + 1..end_geom_idx {
            let (start_coord_idx, end_coord_idx) = self.ring_offsets.start_end(ring_idx);
            let mut ring: Vec<Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
            for coord_idx in start_coord_idx..end_coord_idx {
                ring.push(Coord {
                    x: self.x.value(coord_idx),
                    y: self.y.value(coord_idx),
                })
            }
            interior_rings.push(ring.into());
        }

        Some(Polygon::new(exterior_ring, interior_rings))
    }

    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        // TODO: handle this error
        self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PolygonArray<'a>(pub &'a ListArray<i64>);

impl<'a> PolygonArray<'a> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, i: usize) -> Option<PolygonScalar> {
        if self.0.is_null(i) {
            return None;
        }

        let polygon_value = self.0.value(i);
        let polygon_item = polygon_value
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        Some(PolygonScalar(polygon_item.clone()))
    }

    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        let polygon_item = self.get(i);
        polygon_item.map(|p| p.into_geo())
    }

    pub fn parts(&self) -> PolygonArrayParts<'a> {
        let geom_offsets = self.0.offsets();
        let validity = self.0.validity();

        let inner_dyn_array = self.0.values();
        let inner_array = inner_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let ring_offsets = inner_array.offsets();
        let coords_dyn_array = inner_array.values();
        let coords_array = coords_dyn_array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let x_array_values = coords_array.values()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = coords_array.values()[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        PolygonArrayParts {
            x: x_array_values,
            y: y_array_values,
            ring_offsets,
            geom_offsets,
            validity,
        }
    }
}

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
