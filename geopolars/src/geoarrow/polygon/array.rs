use geo::{LineString, Polygon};
use polars::export::arrow::array::{Array, ListArray, StructArray};
use polars::prelude::Series;

use crate::geoarrow::linestring::array::LineStringScalar;
use crate::util::index_to_chunked_index;

/// A struct representing a non-null single LineString geometry
#[derive(Debug, Clone)]
pub struct PolygonScalar(ListArray<i64>);

impl PolygonScalar {
    pub fn into_geo(self) -> Polygon {
        let exterior_ring = self
            .0
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let exterior_linestring = LineStringScalar(exterior_ring.clone()).into_geo();

        let n_interior_rings = self.0.len();
        let mut interior_rings: Vec<LineString<f64>> = Vec::with_capacity(n_interior_rings - 1);
        for i in 0..n_interior_rings {
            let interior_ring = self
                .0
                .value(i + 1)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            interior_rings.push(LineStringScalar(interior_ring.clone()).into_geo());
        }

        Polygon::new(exterior_linestring, interior_rings)
    }
}

#[derive(Debug, Clone)]
pub struct PolygonArray(ListArray<i64>);

impl PolygonArray {
    pub fn get(&self, i: usize) -> Option<PolygonScalar> {
        if self.0.is_null(i) {
            return None;
        }

        let polygon_item = self
            .0
            .value(i)
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();
        Some(PolygonScalar(polygon_item.clone()))
    }

    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        let polygon_item = self.get(i);

        if let Some(polygon_item) = polygon_item {
            Some(polygon_item.into_geo())
        } else {
            return None;
        }
    }
}

#[derive(Debug, Clone)]
pub struct PolygonSeries(Series);

impl PolygonSeries {
    pub fn get(&self, i: usize) -> Option<PolygonScalar> {
        let (chunk_idx, local_idx) = index_to_chunked_index(&self.0, i);
        let chunk = self.0.chunks()[chunk_idx];

        let polygon_array = PolygonArray(
            chunk
                .as_any()
                .downcast_ref::<ListArray<i64>>()
                .unwrap()
                .clone(),
        );
        polygon_array.get(local_idx)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        let polygon_item = self.get(i);

        if let Some(polygon_item) = polygon_item {
            Some(polygon_item.into_geo())
        } else {
            return None;
        }
    }
}
