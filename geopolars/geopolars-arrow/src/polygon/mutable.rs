use geo::Polygon;
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use polars::export::arrow::datatypes::DataType;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::ArrowField;

#[derive(Debug, Clone)]
pub struct MutablePolygonArray {
    x: Vec<f64>,
    y: Vec<f64>,
    ring_offsets: Vec<i64>,
    geom_offsets: Vec<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl MutablePolygonArray {
    pub fn into_arrow(self) -> ListArray<i64> {
        // Data type
        let coord_field_x = ArrowField::new("x", DataType::Float64, false);
        let coord_field_y = ArrowField::new("y", DataType::Float64, false);
        let struct_data_type = DataType::Struct(vec![coord_field_x, coord_field_y]);
        let inner_list_data_type = DataType::LargeList(Box::new(ArrowField::new(
            "vertices",
            struct_data_type.clone(),
            false,
        )));
        let outer_list_data_type = DataType::LargeList(Box::new(ArrowField::new(
            "rings",
            inner_list_data_type.clone(),
            false,
        )));

        // Validity
        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        // Offsets
        let ring_offsets_buffer = unsafe { OffsetsBuffer::new_unchecked(self.ring_offsets.into()) };
        let geom_offsets_buffer = unsafe { OffsetsBuffer::new_unchecked(self.geom_offsets.into()) };

        // Array data
        let array_x = Box::new(PrimitiveArray::<f64>::from_vec(self.x)) as Box<dyn Array>;
        let array_y = Box::new(PrimitiveArray::<f64>::from_vec(self.y)) as Box<dyn Array>;

        let coord_array = Box::new(StructArray::new(
            struct_data_type,
            vec![array_x, array_y],
            None,
        )) as Box<dyn Array>;

        let inner_list_array = Box::new(ListArray::new(
            inner_list_data_type,
            ring_offsets_buffer,
            coord_array,
            None,
        )) as Box<dyn Array>;

        ListArray::new(
            outer_list_data_type,
            geom_offsets_buffer,
            inner_list_array,
            validity,
        )
    }
}

impl From<Vec<Polygon>> for MutablePolygonArray {
    fn from(geoms: Vec<Polygon>) -> Self {
        use geo::coords_iter::CoordsIter;

        // Offset into ring indexes for each geometry
        let mut geom_offsets: Vec<i64> = Vec::with_capacity(geoms.len() + 1);
        geom_offsets.push(0);

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets: Vec<i64> = Vec::with_capacity(geoms.len() + 1);
        ring_offsets.push(0);

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            // Total number of rings in this polygon
            current_geom_offset += geom.interiors().len() + 1;
            geom_offsets.push(current_geom_offset as i64);

            // Number of coords for each ring
            current_ring_offset += geom.exterior().coords_count();
            ring_offsets.push(current_ring_offset as i64);

            for int_ring in geom.interiors() {
                current_ring_offset += int_ring.coords_count();
                ring_offsets.push(current_ring_offset as i64);
            }
        }

        let mut x_arr = Vec::<f64>::with_capacity(current_ring_offset);
        let mut y_arr = Vec::<f64>::with_capacity(current_ring_offset);

        for geom in geoms {
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    x_arr.push(coord.x);
                    y_arr.push(coord.y);
                }
            }
        }

        MutablePolygonArray {
            x: x_arr,
            y: y_arr,
            geom_offsets,
            ring_offsets,
            validity: None,
        }
    }
}

impl From<Vec<Option<Polygon>>> for MutablePolygonArray {
    fn from(geoms: Vec<Option<Polygon>>) -> Self {
        use geo::coords_iter::CoordsIter;

        let mut validity = MutableBitmap::with_capacity(geoms.len());

        // Offset into ring indexes for each geometry
        let mut geom_offsets: Vec<i64> = Vec::with_capacity(geoms.len() + 1);
        geom_offsets.push(0);

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets: Vec<i64> = Vec::with_capacity(geoms.len() + 1);
        ring_offsets.push(0);

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            if let Some(geom) = geom {
                validity.push(true);

                // Total number of rings in this polygon
                current_geom_offset += geom.interiors().len() + 1;
                geom_offsets.push(current_geom_offset as i64);

                // Number of coords for each ring
                current_ring_offset += geom.exterior().coords_count();
                ring_offsets.push(current_ring_offset as i64);

                for int_ring in geom.interiors() {
                    current_ring_offset += int_ring.coords_count();
                    ring_offsets.push(current_ring_offset as i64);
                }
            } else {
                validity.push(false);
                geom_offsets.push(current_geom_offset as i64);
            }
        }

        let mut x_arr = Vec::<f64>::with_capacity(current_ring_offset);
        let mut y_arr = Vec::<f64>::with_capacity(current_ring_offset);

        for geom in geoms.into_iter().flatten() {
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    x_arr.push(coord.x);
                    y_arr.push(coord.y);
                }
            }
        }

        MutablePolygonArray {
            x: x_arr,
            y: y_arr,
            geom_offsets,
            ring_offsets,
            validity: Some(validity),
        }
    }
}
