use crate::error::GeoArrowError;
use crate::linestring::array::check;
use crate::multipoint::MutableMultiPointArray;
use crate::LineStringArray;
use geo::{CoordsIter, LineString};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use polars::export::arrow::datatypes::DataType;
use polars::export::arrow::offset::{Offsets, OffsetsBuffer};
use polars::prelude::ArrowField;
use std::convert::From;
use std::vec;

/// The Arrow equivalent to `Vec<Option<LineString>>`.
/// Converting a [`MutableLineStringArray`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableLineStringArray {
    x: Vec<f64>,
    y: Vec<f64>,
    geom_offsets: Offsets<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl MutableLineStringArray {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(capacity),
            y: Vec::with_capacity(capacity),
            geom_offsets: Offsets::<i64>::with_capacity(0),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableLineStringArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn try_new(
        x: Vec<f64>,
        y: Vec<f64>,
        geom_offsets: Offsets<i64>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> (Vec<f64>, Vec<f64>, Offsets<i64>, Option<MutableBitmap>) {
        (self.x, self.y, self.geom_offsets, self.validity)
    }

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
        let array_x = Box::new(PrimitiveArray::<f64>::from_vec(self.x)) as Box<dyn Array>;
        let array_y = Box::new(PrimitiveArray::<f64>::from_vec(self.y)) as Box<dyn Array>;

        let coord_array = Box::new(StructArray::new(
            struct_data_type,
            vec![array_x, array_y],
            None,
        )) as Box<dyn Array>;

        // Offsets
        let offsets_buffer: OffsetsBuffer<i64> = self.geom_offsets.into();

        ListArray::new(list_data_type, offsets_buffer, coord_array, validity)
    }
}

impl Default for MutableLineStringArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutableLineStringArray> for LineStringArray {
    fn from(other: MutableLineStringArray) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        Self::new(
            other.x.into(),
            other.y.into(),
            other.geom_offsets.into(),
            validity,
        )
    }
}

impl From<MutableLineStringArray> for ListArray<i64> {
    fn from(arr: MutableLineStringArray) -> Self {
        arr.into_arrow()
    }
}

// TODO: in the future it would be useful to DRY the functions here and for MultiPoint

/// Implement a converter that can be used for either Vec<LineString> or
/// Vec<MultiPoint>
pub(crate) fn line_string_from_geo_vec(geoms: Vec<LineString>) -> MutableLineStringArray {
    let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

    let mut current_offset = 0;
    for geom in &geoms {
        current_offset += geom.coords_count();
        geom_offsets.try_push_usize(current_offset).unwrap();
    }

    let mut x_arr = Vec::<f64>::with_capacity(current_offset);
    let mut y_arr = Vec::<f64>::with_capacity(current_offset);

    for geom in geoms {
        for coord in geom.coords_iter() {
            x_arr.push(coord.x);
            y_arr.push(coord.y);
        }
    }

    MutableLineStringArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: None,
    }
}

/// Implement a converter that can be used for either Vec<Option<LineString>> or
/// Vec<Option<MultiPoint>>
pub(crate) fn line_string_from_geo_option_vec(
    geoms: Vec<Option<LineString>>,
) -> MutableLineStringArray {
    let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

    let mut validity = MutableBitmap::with_capacity(geoms.len());

    let mut current_offset = 0;
    for maybe_geom in &geoms {
        if let Some(geom) = maybe_geom {
            current_offset += geom.coords_count();
            validity.push(true);
        } else {
            validity.push(false);
        }
        geom_offsets.try_push_usize(current_offset).unwrap();
    }

    let mut x_arr = Vec::<f64>::with_capacity(current_offset);
    let mut y_arr = Vec::<f64>::with_capacity(current_offset);

    for geom in geoms.into_iter().flatten() {
        for coord in geom.coords_iter() {
            x_arr.push(coord.x);
            y_arr.push(coord.y);
        }
    }

    MutableLineStringArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: Some(validity),
    }
}

impl From<Vec<LineString>> for MutableLineStringArray {
    fn from(geoms: Vec<LineString>) -> Self {
        line_string_from_geo_vec(geoms)
    }
}

impl From<Vec<Option<LineString>>> for MutableLineStringArray {
    fn from(geoms: Vec<Option<LineString>>) -> Self {
        line_string_from_geo_option_vec(geoms)
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<MutableLineStringArray> for MutableMultiPointArray {
    fn from(value: MutableLineStringArray) -> Self {
        Self::try_new(value.x, value.y, value.geom_offsets, value.validity).unwrap()
    }
}
