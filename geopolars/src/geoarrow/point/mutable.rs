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
    fn len(&self) -> usize {
        self.x.len()
    }

    /// Creates a new [`MutablePointArray`] from a capacity and [`DataType`].
    pub fn with_capacity(capacity: usize) -> Self {
        MutablePointArray {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
            validity: None,
        }
    }

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

    /// Adds a new value to the array.
    pub fn push(&mut self, value: Option<Point>) {
        match value {
            Some(value) => {
                self.x.push(value.x());
                self.y.push(value.y());
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.x.push(0.0);
                self.y.push(0.0);
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {
                        self.init_validity();
                    }
                }
            }
        }
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.x.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }
}

impl From<MutablePointArray> for StructArray {
    fn from(arr: MutablePointArray) -> Self {
        arr.into_arrow()
    }
}

impl From<Vec<Point>> for MutablePointArray {
    fn from(geoms: Vec<Point>) -> Self {
        let mut x_arr = Vec::<f64>::with_capacity(geoms.len());
        let mut y_arr = Vec::<f64>::with_capacity(geoms.len());

        for geom in geoms {
            x_arr.push(geom.x());
            y_arr.push(geom.y());
        }

        MutablePointArray {
            x: x_arr,
            y: y_arr,
            validity: None,
        }
    }
}

impl From<Vec<Option<Point>>> for MutablePointArray {
    fn from(geoms: Vec<Option<Point>>) -> Self {
        let mut x_arr = vec![0.0_f64; geoms.len()];
        let mut y_arr = vec![0.0_f64; geoms.len()];
        let mut validity = MutableBitmap::with_capacity(geoms.len());

        for i in 0..geoms.len() {
            if let Some(geom) = geoms[i] {
                x_arr[i] = geom.x();
                y_arr[i] = geom.y();
                validity.push(true);
            } else {
                validity.push(false);
            }
        }

        MutablePointArray {
            x: x_arr,
            y: y_arr,
            validity: Some(validity),
        }
    }
}
