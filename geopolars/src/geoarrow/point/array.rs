use geo::Point;
use polars::export::arrow::array::{Array, PrimitiveArray, StructArray};
use polars::prelude::Series;

use crate::util::index_to_chunked_index;

#[derive(Debug, Clone)]
pub struct PointArray(StructArray);

impl PointArray {
    pub fn get_as_geo(&self, i: usize) -> Option<Point> {
        if self.0.is_null(i) {
            return None;
        }

        let struct_array_values = self.0.values();
        let x_arrow_array = &struct_array_values[0];
        let y_arrow_array = &struct_array_values[1];

        let x_array_values = x_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = y_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Some(Point::new(x_array_values.value(i), y_array_values.value(i)))
    }
}

#[derive(Debug, Clone)]
pub struct PointSeries(Series);

impl PointSeries {
    pub fn get_as_geo(&self, i: usize) -> Option<Point> {
        let (chunk_idx, local_idx) = index_to_chunked_index(&self.0, i);
        let chunk = self.0.chunks()[chunk_idx];

        let pa = PointArray(
            chunk
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone(),
        );
        pa.get_as_geo(local_idx)
    }
}
