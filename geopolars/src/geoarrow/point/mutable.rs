use polars::export::arrow::array::{Array, MutableArray, StructArray};
use polars::export::arrow::bitmap::MutableBitmap;
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
        let struct_data_type = DataType::Struct(vec![field_x, field_y]);
        let struct_values: Vec<Box<dyn Array>> = vec![self.x.into(), self.y.into()];

        StructArray::new(struct_data_type, struct_values, self.validity.into())
    }
}
