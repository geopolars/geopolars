use geo::Point;
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

        let array_x = Box::new(PrimitiveArray::<f64>::from_vec(self.x)) as Box<dyn Array>;
        let array_y = Box::new(PrimitiveArray::<f64>::from_vec(self.y)) as Box<dyn Array>;

        let struct_data_type = DataType::Struct(vec![field_x, field_y]);
        let struct_values = vec![array_x, array_y];

        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        StructArray::new(struct_data_type, struct_values, validity)
    }

    pub fn push(&mut self, p: Point) {
        self.x.push(p.x());
        self.y.push(p.y());
    }
}

// /// Setters
// impl MutablePointArray {
//     /// Sets position `index` to `value`.
//     /// Note that if it is the first time a null appears in this array,
//     /// this initializes the validity bitmap (`O(N)`).
//     /// # Panic
//     /// Panics iff index is larger than `self.len()`.
//     pub fn set(&mut self, index: usize, value: Option<(f64, f64)>) {
//         self.x.set(index, value.u);
//         self.y.set(index, value);
//         assert!(index < self.len());
//         // Safety:
//         // we just checked bounds
//         unsafe { self.set_unchecked(index, value) }
//     }

//     /// Sets position `index` to `value`.
//     /// Note that if it is the first time a null appears in this array,
//     /// this initializes the validity bitmap (`O(N)`).
//     /// # Safety
//     /// Caller must ensure `index < self.len()`
//     pub unsafe fn set_unchecked(&mut self, index: usize, value: Option<(f64, f64)>) {
//         *self.values.get_unchecked_mut(index) = value.unwrap_or_default();

//         if value.is_none() && self.validity.is_none() {
//             // When the validity is None, all elements so far are valid. When one of the elements is set fo null,
//             // the validity must be initialized.
//             let mut validity = MutableBitmap::new();
//             validity.extend_constant(self.len(), true);
//             self.validity = Some(validity);
//         }
//         if let Some(x) = self.validity.as_mut() {
//             x.set_unchecked(index, value.is_some())
//         }
//     }

//     /// Sets the validity.
//     /// # Panic
//     /// Panics iff the validity's len is not equal to the existing values' length.
//     pub fn set_validity(&mut self, validity: Option<MutableBitmap>) {
//         if let Some(validity) = &validity {
//             assert_eq!(self.values.len(), validity.len())
//         }
//         self.validity = validity;
//     }
// }
