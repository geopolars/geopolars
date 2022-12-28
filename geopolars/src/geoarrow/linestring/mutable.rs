use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use polars::export::arrow::datatypes::DataType;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::ArrowField;

#[derive(Debug, Clone)]
pub struct MutableLineStringArray {
    x: Vec<f64>,
    y: Vec<f64>,
    offsets: Vec<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl MutableLineStringArray {
    pub fn into_arrow(self) -> ListArray<i64> {
        // Data type
        let coord_field_x = ArrowField::new("x", DataType::Float64, false);
        let coord_field_y = ArrowField::new("y", DataType::Float64, false);
        let struct_data_type = DataType::Struct(vec![coord_field_x, coord_field_y]);
        let list_data_type = DataType::LargeList(Box::new(ArrowField::new(
            "vertices",
            struct_data_type.clone(),
            false,
        )));

        // Validity
        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        // Array data
        let array_x = Box::new(PrimitiveArray::<f64>::from_values(self.x)) as Box<dyn Array>;
        let array_y = Box::new(PrimitiveArray::<f64>::from_values(self.y)) as Box<dyn Array>;

        let coord_array = Box::new(StructArray::new(
            struct_data_type,
            vec![array_x, array_y],
            None,
        )) as Box<dyn Array>;

        // Offsets
        let offsets_buffer = unsafe { OffsetsBuffer::new_unchecked(self.offsets.into()) };

        ListArray::new(list_data_type, offsets_buffer, coord_array, validity)
    }
}
