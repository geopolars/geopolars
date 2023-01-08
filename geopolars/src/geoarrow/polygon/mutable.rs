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
