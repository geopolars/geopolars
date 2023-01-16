use geo::Point;
use polars::export::arrow::array::StructArray;
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};

use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::trait_::MutableGeometryArray;

use super::array::{check, PointArray};

/// The Arrow equivalent to `Vec<Option<Point>>`.
/// Converting a [`MutablePointArray`] into a [`PointArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutablePointArray {
    x: Vec<f64>,
    y: Vec<f64>,
    validity: Option<MutableBitmap>,
}

impl MutablePointArray {
    /// Creates a new empty [`MutablePointArray`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new [`MutablePointArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutablePointArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    /// * The `data_type`'s [`crate::datatypes::PhysicalType`] is not equal to [`crate::datatypes::PhysicalType::Primitive(T::PRIMITIVE)`]
    pub fn try_new(
        x: Vec<f64>,
        y: Vec<f64>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self { x, y, validity })
    }

    /// Extract the low-level APIs from the [`MutablePointArray`].
    pub fn into_inner(self) -> (Vec<f64>, Vec<f64>, Option<MutableBitmap>) {
        (self.x, self.y, self.validity)
    }

    /// Adds a new value to the array.
    pub fn push_geo(&mut self, value: Option<Point>) {
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
                self.x.push(f64::default());
                self.y.push(f64::default());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {
                        self.init_validity();
                    }
                }
            }
        }
    }

    /// Pop a value from the array.
    /// Note if the values is empty, this method will return None.
    pub fn pop_geo(&mut self) -> Option<Point> {
        let x = self.x.pop()?;
        let y = self.y.pop()?;
        let pt = Point::new(x, y);

        self.validity
            .as_mut()
            .map(|x| x.pop()?.then_some(pt))
            .unwrap_or_else(|| Some(pt))
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.x.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }
}

impl MutablePointArray {
    fn len(&self) -> usize {
        self.x.len()
    }

    pub fn into_arrow(self) -> StructArray {
        let point_array: PointArray = self.into();
        point_array.into_arrow()
    }
}

impl MutableGeometryArray for MutablePointArray {
    fn geometry_type(&self) -> GeometryType {
        GeometryType::Point
    }

    fn len(&self) -> usize {
        self.x.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl Default for MutablePointArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutablePointArray> for PointArray {
    fn from(other: MutablePointArray) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        Self::new(other.x.into(), other.y.into(), validity)
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
