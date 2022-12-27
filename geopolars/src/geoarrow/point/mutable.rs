use polars::export::arrow::array::{Array, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use polars::export::arrow::datatypes::DataType;
use polars::prelude::ArrowField;

#[derive(Debug, Clone)]
pub struct MutablePointArray {
    x: Vec<f64>,
    y: Vec<f64>,
    validity: Option<MutableBitmap>,
}

impl MutablePointArray {
    pub fn into_arrow(self) -> StructArray {
        let field_x = ArrowField::new("x", DataType::Float64, false);
        let field_y = ArrowField::new("y", DataType::Float64, false);

        let array_x = Box::new(PrimitiveArray::<f64>::from_values(self.x)) as Box<dyn Array>;
        let array_y = Box::new(PrimitiveArray::<f64>::from_values(self.y)) as Box<dyn Array>;

        let struct_data_type = DataType::Struct(vec![field_x, field_y]);
        let struct_values = vec![array_x, array_y];

        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        StructArray::new(struct_data_type, struct_values, validity)
    }
}
